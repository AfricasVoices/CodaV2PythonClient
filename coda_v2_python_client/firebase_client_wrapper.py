import json

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from core_data_modules.logging import Logger
from core_data_modules.data_models import Message
from core_data_modules.data_models import CodeScheme
from core_data_modules.data_models import MessagesMetrics

log = Logger(__name__)


class CodaV2Client:
    MAX_SEGMENT_SIZE = 2500

    def __init__(self, client):
        """
        Inits Coda V2 client

        :param client: Client for interacting with Google Cloud Firestore.
        :type client: google.cloud.firestore.Firestore
        """
        self._client = client

    @classmethod
    def init_client(cls, crypto_token_path, app_name="CodaV2Client"):
        """
        Inits Coda V2 client

        :param crypto_token_path: Path to the Firestore credentials file
        :type crypto_token_path: str
        :param app_name: Name to call the Firestore app instance we'll use to connect, defaults to "CodaV2Client"
        :type app_name: str, optional
        :return: Coda V2 client instance
        :rtype: CodaV2Client
        """
        try:
            firebase_admin.get_app()
        except ValueError:
            log.debug("Creating default Firebase app")
            firebase_admin.initialize_app()

        log.debug(f"Creating Firebase app {app_name}")
        cred = credentials.Certificate(crypto_token_path)
        app = firebase_admin.initialize_app(cred, name=app_name)
        return cls(firestore.client(app))

    def transaction(self):
        """
        Returns a firestore function for performing a set of read and write operations on one or more documents.

        :return: One of the helpers for applying Google Cloud Firestore changes in a transaction.
        :rtype: google.cloud.firestore.Transaction
        """
        return self._client.transaction()

    def get_dataset_ids(self):
        """
        Gets all the available dataset ids in Coda (For each segmented dataset, returns only the primary dataset id).

        :return: Ids of all the available datasets.
        :rtype: set of str
        """
        segment_ids = self.get_segment_ids()
        assert len(segment_ids) == len(set(segment_ids)), "Segment ids not unique"

        dataset_ids = set(segment_ids)
        for dataset_id in self.get_segmented_dataset_ids():
            segment_count = self.get_segment_count(dataset_id)
            if segment_count is not None and segment_count > 1:
                for segment_index in range(2, segment_count + 1):
                    dataset_ids.remove(self.id_for_segment(dataset_id, segment_index))

        return dataset_ids

    def get_segment_ids(self):
        """
        Gets ids of all segments (including for datasets that contain only one segment)

        :return: Ids of all segments.
        :rtype: list of str
        """
        ids = []
        for segment in self._client.collection("datasets").get():
            ids.append(segment.id)
        return ids

    @staticmethod
    def id_for_segment(dataset_id, segment_index=None):
        """
        Gets the id for segment `n` of a dataset.

        :param dataset_id: Id of a dataset
        :type dataset_id: str
        :param segment_index: Segment `n` of a dataset, defaults to None
        :type segment_index: int, optional
        :return: Id for segment `n` of a dataset.
        :rtype: str
        """
        if segment_index is None or segment_index == 1:
            return dataset_id
        return f"{dataset_id}_{segment_index}"

    def get_segmented_dataset_ids(self):
        """
        Gets segmented dataset ids

        :return: Ids of all datasets that are segmented
        :rtype: list of str
        """
        segmented_dataset_ids = []
        for doc in self._client.collection("segment_counts").get():
            segmented_dataset_ids.append(doc.id)
        return segmented_dataset_ids

    def get_dataset_segment_count_ref(self, dataset_id):
        """
        Gets Firestore database reference to segment count document.

        :param dataset_id: Id of a dataset
        :type dataset_id: str
        :return: A reference to a document in a Firestore database
        :rtype: google.cloud.firestore.DocumentReference
        """
        return self._client.document(f"segment_counts/{dataset_id}")

    def get_segment_count(self, dataset_id, transaction=None):
        """
        Gets number of segments for a given dataset. If the dataset is not segmented, returns 1

        :param dataset_id: Id of a dataset
        :type dataset_id: str
        :param transaction: Transaction to run this get in.
        :type transaction: google.cloud.firestore.Transaction | None
        :return: Number of segments for a given dataset
        :rtype: int
        """
        segment_count_doc = self.get_dataset_segment_count_ref(dataset_id).get(transaction=transaction).to_dict()
        if segment_count_doc is None:
            return 1
        return segment_count_doc["segment_count"]

    def set_segment_count(self, dataset_id, segment_count, transaction=None): #TODO: Rename to set_dataset_segment_count
        """
        Sets number of segments for a given dataset.

        :param dataset_id: Id of a dataset
        :type dataset_id: str
        :param segment_count: Number of segment for a given dataset.
        :type segment_count: int
        """
        if transaction is None:
            self.get_dataset_segment_count_ref(dataset_id).set({"segment_count": segment_count})
        else:
            transaction.set(self.get_dataset_segment_count_ref(dataset_id), {"segment_count": segment_count})

    def create_next_segment(self, dataset_id, transaction=None):
        """
        Creates a new segment for a given dataset

        :param dataset_id: Id of the dataset to create segment for.
        :type dataset_id: str
        :param transaction: Transaction to run this get in.
        :type transaction: google.cloud.firestore.Transaction
        """
        segment_count = self.get_segment_count(dataset_id, transaction=transaction)
        current_segment_id = self.id_for_segment(dataset_id, segment_count)

        next_segment_count = segment_count + 1
        next_segment_id = self.id_for_segment(dataset_id, next_segment_count)

        log.debug(f"Creating next dataset segment with id {next_segment_id}")

        code_schemes = self.get_all_code_schemes(current_segment_id, transaction=transaction)
        user_ids = self.get_dataset_user_ids(current_segment_id, transaction=transaction)

        self.add_and_update_segment_code_schemes(next_segment_id, code_schemes, transaction=transaction)
        if user_ids is not None:
            self.set_segment_user_ids(next_segment_id, user_ids, transaction=transaction)
        self.set_segment_count(dataset_id, next_segment_count, transaction=transaction)

        if transaction is None:
            for x in range(0, 10):
                if self.get_segment_count(dataset_id) == next_segment_count:
                    return
                log.debug("New segment count not yet committed, waiting 1s before retrying")
                time.sleep(1)
            assert False, "Server segment count did not update to the newest count fast enough"

    def get_message_ref(self, segment_id, message_id):
        """
        Gets Firestore database reference to a message.

        :param segment_id: Id of a segment
        :type segment_id: str
        :param message_id: Id of a message
        :type message_id: str
        :return: A reference to a document in a Firestore database
        :rtype: google.cloud.firestore.DocumentReference
        """
        return self._client.document(f"datasets/{segment_id}/messages/{message_id}")

    def get_messages_ref(self, segment_id):
        """
        Gets Firestore database reference to messages.

        :param segment_id: Id of a segment
        :type segment_id: str
        :return: A reference to collection `messages` in Firestore database
        :rtype: google.cloud.firestore.CollectionReference
        """
        return self._client.collection(f"datasets/{segment_id}/messages")

    def get_segment_message(self, segment_id, message_id, transaction=None):
        """
        Gets a message from a segment by id. If the message is not found, returns None.

        :param segment_id: Id of a segment.
        :type segment_id: str
        :param message_id: Id of a message.
        :type message_id: str
        :param transaction: Transaction to run this get in.
        :type transaction: google.cloud.firestore.Transaction
        :return: A message from a segment.
        :rtype: core_data_modules.data_models.message.Message | None
        """
        message_snapshot = self.get_message_ref(segment_id, message_id).get(transaction=transaction)
        if message_snapshot.exists:
            return Message.from_firebase_map(message_snapshot.to_dict())
        return None

    def update_segment_message(self, segment_id, message, transaction):
        """
        Updates an existing message in a segment.

        :param segment_id: Id of the segment that contains the message to update.
        :type segment_id: str
        :param message: Message to update.
        :type message: core_data_modules.data_models.message.Message
        :param transaction: Transaction to run this set in.
        :type transaction: google.cloud.firestore.Transaction
        """
        # Check the message already exists in this segment.
        old_message = self.get_segment_message(segment_id, message.message_id, transaction=transaction)
        if old_message is None:
            raise ValueError(f"Message {message.message_id} not found in segment {segment_id}")

        # Get the segment's current metrics.
        segment_metrics = self.get_segment_messages_metrics(segment_id, transaction=transaction)
        segment_code_schemes = self.get_all_code_schemes(segment_id, transaction=transaction)

        # Update the message.
        transaction.set(self.get_message_ref(segment_id, message.message_id), message.to_firebase_map())

        # Update the segment's metrics.
        segment_metrics -= CodaV2Client.compute_message_metrics(old_message, segment_code_schemes)
        segment_metrics += CodaV2Client.compute_message_metrics(message, segment_code_schemes)
        self.set_segment_messages_metrics(segment_id, segment_metrics, transaction=transaction)

    def update_dataset_message(self, dataset_id, message, transaction):
        segment_id = self.get_segment_id_for_message_id(dataset_id, message.message_id, transaction=transaction)
        if segment_id is None:
            self.add_message_to_dataset(dataset_id, message)
            return

        self.update_segment_message(segment_id, message, transaction=transaction)

    def get_segment_id_for_message_id(self, dataset_id, message_id, transaction):
        segment_count = self.get_segment_count(dataset_id, transaction=transaction)
        for segment_index in range(1, segment_count + 1):
            segment_id = self.id_for_segment(dataset_id, segment_index)
            message = self.get_segment_message(segment_id, message_id, transaction=transaction)
            if message is not None:
                return segment_id

        return None

    def get_segment_messages(self, segment_id, last_updated_after=None, last_updated_before=None, transaction=None):
        """
        Downloads messages from the requested segment, optionally filtering by when the messages were last updated.

        If filtering by when the messages where last updated, only message objects which contain a LastUpdated field
        will be returned.

        :param segment_id: Id of segment to download messages from.
        :type segment_id: str
        :param last_updated_after: If specified, filters the downloaded messages to only include messages with a LastUpdated
                                   field and where the LastUpdated field is later than last_updated_after. Defaults to None.
        :type last_updated_after: datetime, optional
        :param last_updated_before: If specified, filters the downloaded messages to only include messages with a LastUpdated
                                    field and where the LastUpdated field is earlier than, or the same time as,
                                    last_updated_before. Defaults to None
        :type last_updated_before: datetime, optional
        :param transaction: Transaction to run this get in.
        :type transaction: google.cloud.firestore.Transaction
        :return: Messages in this segment, filtered by 'LastUpdated' timestamp if requested.
        :rtype: list of core_data_modules.data_models.message.Message
        """
        messages_ref = self.get_messages_ref(segment_id)
        if last_updated_after is not None:
            messages_ref = messages_ref.where("LastUpdated", ">", last_updated_after)
        if last_updated_before is not None:
            messages_ref = messages_ref.where("LastUpdated", "<=", last_updated_before)
        raw_messages = [message.to_dict() for message in messages_ref.get(transaction=transaction)]

        return [Message.from_firebase_map(message) for message in raw_messages]

    def get_dataset_message(self, dataset_id, message_id, transaction=None):
        """
        Gets a message from a dataset by id. If the message is not found, returns None.

        :param dataset_id: Id of a dataset.
        :type dataset_id: str
        :param message_id: Id of a message.
        :type message_id: str
        :param transaction: Transaction to run this get in.
        :type transaction: google.cloud.firestore.Transaction
        :return: A message from a dataset.
        :rtype: core_data_modules.data_models.message.Message | None
        """
        segment_count = self.get_segment_count(dataset_id)
        for segment_index in range(1, segment_count + 1):
            segment_id = self.id_for_segment(dataset_id, segment_index)
            message = self.get_segment_message(segment_id, message_id, transaction=transaction)
            if message is not None:
                log.debug(f"Message found in segment {segment_id}")
                return message

        return None

    def get_dataset_messages(self, dataset_id, last_updated_after=None):
        """
        Downloads messages from the requested dataset, optionally filtering by when the messages were last updated.

        If filtering by when the messages where last updated, only message objects which contain a LastUpdated field
        will be returned.

        :param dataset_id: Id of dataset to download messages from.
        :type dataset_id: str
        :param last_updated_after: If specified, filters the downloaded messages to only include messages with a LastUpdated
                                   field and where the LastUpdated field is later than last_updated_after. Defaults to None.
        :type last_updated_after: datetime, optional
        :return: Messages in this dataset, filtered by 'LastUpdated' timestamp if requested.
        :rtype: list of core_data_modules.data_models.message.Message
        """
        segment_count = self.get_segment_count(dataset_id)
        if segment_count == 1:
            return self.get_segment_messages(dataset_id, last_updated_after)
        else:
            # Get the messages for each segment
            messages_by_segment = dict()  # of segment id -> list of message
            for segment_index in range(1, segment_count + 1):
                segment_id = self.id_for_segment(dataset_id, segment_index)
                messages_by_segment[segment_id] = self.get_segment_messages(segment_id, last_updated_after)

            # Search the fetched segments for the most and least recently updated timestamps in all the segments downloaded
            # above.
            dataset_first_updated = None
            dataset_last_updated = None
            for segment_messages in messages_by_segment.values():
                for msg in segment_messages:
                    if msg.last_updated is not None:
                        if dataset_last_updated is None or msg.last_updated > dataset_last_updated:
                            dataset_last_updated = msg.last_updated
                        if dataset_first_updated is None or msg.last_updated < dataset_first_updated:
                            dataset_first_updated = msg.last_updated

            # Check all the segments for any messages between the latest one we fetched above and the most recently updated
            # message seen in any segment. If we didn't fetch any new messages for a segment, use the oldest timestamp
            # across all segments (dataset_first_updated) instead. This is to ensure we don't miss any messages that were
            # being labelled while we were pulling the separate segments, and is needed to maintain the consistency
            # guarantees we need for incremental fetch.
            for segment_id, segment_messages in messages_by_segment.items():
                segment_last_updated = None
                for msg in segment_messages:
                    if msg.last_updated is not None:
                        if segment_last_updated is None or msg.last_updated > segment_last_updated:
                            segment_last_updated = msg.last_updated

                if segment_last_updated is None:
                    segment_last_updated = dataset_first_updated

                if segment_last_updated is not None:
                    updated_segment_messages = self.get_segment_messages(segment_id, last_updated_after=segment_last_updated, last_updated_before=dataset_last_updated)  # nopep8
                    messages_by_segment[segment_id].extend(updated_segment_messages)

            # Combine all the messages downloaded from each segment.
            messages = []
            for segment_messages in messages_by_segment.values():
                messages.extend(segment_messages)

            # Check that there are no duplicate message ids.
            seen_message_ids = set()
            for message in messages:
                assert message.message_id not in seen_message_ids, "Duplicate message found"
                seen_message_ids.add(message.message_id)

            return messages

    def get_code_schemes_ref(self, segment_id):
        """
        Gets Firestore database reference to code schemes.

        :param segment_id: Id of a segment.
        :type segment_id: str
        :return: A reference to collection `code_schemes` in Firestore database
        :rtype: google.cloud.firestore.CollectionReference
        """
        return self._client.collection(f"datasets/{segment_id}/code_schemes")

    def ensure_code_schemes_consistent(self, dataset_id, transaction=None):
        """
        Checks that the code schemes are the same in all segments

        :param dataset_id: Id of a dataset.
        :type dataset_id: str
        :param transaction: Transaction to run this get in.
        :type transaction: google.cloud.firestore.Transaction
        """
        segment_count = self.get_segment_count(dataset_id, transaction=transaction)
        if segment_count == 1:
            return

        first_segment_schemes = []
        for code_scheme in self.get_code_schemes_ref(dataset_id).get():
            first_segment_schemes.append(CodeScheme.from_firebase_map(code_scheme.to_dict()))

        for segment_index in range(2, segment_count + 1):
            segment_id = self.id_for_segment(dataset_id, segment_index)

            current_segment_schemes = []
            for code_scheme in self.get_code_schemes_ref(segment_id).get():
                current_segment_schemes.append(CodeScheme.from_firebase_map(code_scheme.to_dict()))

            assert len(first_segment_schemes) == len(current_segment_schemes), \
                f"Segment {segment_id} has a different number of schemes to the first segment {dataset_id}"

            first_segment_schemes.sort(key=lambda s: s.scheme_id)
            current_segment_schemes.sort(key=lambda s: s.scheme_id)

            for x, y in zip(first_segment_schemes, current_segment_schemes):
                assert x == y, f"Segment {segment_id} has different schemes to the first segment {dataset_id}"

    def get_all_code_schemes(self, dataset_id, transaction=None):
        """
        Gets all code schemes for a given dataset

        :param dataset_id: Id of a dataset.
        :type dataset_id: str
        :param transaction: Transaction to run this get in.
        :type transaction: google.cloud.firestore.Transaction
        :return: Code schemes in this dataset
        :rtype: list of core_data_modules.data_models.code_scheme.CodeScheme
        """
        self.ensure_code_schemes_consistent(dataset_id, transaction=transaction)

        code_schemes = []
        for doc in self.get_code_schemes_ref(dataset_id).get(transaction=transaction):
            code_schemes.append(CodeScheme.from_firebase_map(doc.to_dict()))
        return code_schemes

    def get_segment_code_scheme_ref(self, segment_id, scheme_id):
        """
        Gets Firestore database reference to a code scheme.

        :param segment_id: Id of a segment that has the code scheme given.
        :type segment_id: str
        :param scheme_id: Id of a code scheme to get the reference for.
        :type scheme_id: str
        :return: A reference to a document in a Firestore database.
        :rtype: google.cloud.firestore.DocumentReference
        """
        return self._client.document(f"datasets/{segment_id}/code_schemes/{scheme_id}")

    def set_dataset_code_scheme(self, dataset_id, code_scheme):
        """
        Sets a code scheme for a given dataset.

        :param dataset_id: Id of the dataset to set the code scheme for.
        :type dataset_id: str
        :param code_scheme: Code scheme to be set.
        :type code_scheme: core_data_modules.data_models.code_scheme.CodeScheme
        """
        scheme_id = code_scheme.scheme_id
        segment_count = self.get_segment_count(dataset_id)

        batch = self._client.batch()

        for segment_index in range(1, segment_count + 1):
            segment_id = self.id_for_segment(dataset_id, segment_index)
            batch.set(self.get_segment_code_scheme_ref(segment_id, scheme_id), code_scheme.to_firebase_map())

        batch.commit()
        log.debug(f"Wrote scheme: {scheme_id}")

    def set_segment_code_scheme(self, segment_id, code_scheme, transaction=None):
        """
        Sets a code scheme for a given segment.

        :param segment_id: Id of the segment to set the code scheme for.
        :type segment_id: str
        :param code_scheme: Code scheme to be set.
        :type code_scheme: core_data_modules.data_models.code_scheme.CodeScheme
        :param transaction: Transaction to run this in or None.
        :type transaction: google.cloud.firestore.Transaction | None
        """
        scheme_id = code_scheme.scheme_id
        if transaction is None:
            # If no transaction was given, run all the updates in a new batched-write transaction and flag that
            # this transaction needs to be committed before returning from this function.
            transaction = self._client.batch()
            commit_before_returning = True
        else:
            commit_before_returning = False

        transaction.set(self.get_segment_code_scheme_ref(segment_id, scheme_id), code_scheme.to_firebase_map())

        if commit_before_returning:
            transaction.commit()

        log.debug(f"Wrote scheme: {scheme_id}")

    def add_and_update_dataset_code_schemes(self, dataset_id, code_schemes):
        """
        Adds or updates code schemes for a given dataset.

        :param dataset_id: Id of the dataset to add or update the code schemes for.
        :type dataset_id: str
        :param code_schemes: Code schemes to be added or updated.
        :type code_schemes: list of core_data_modules.data_models.code_scheme.CodeScheme
        """
        for code_scheme in code_schemes:
            self.set_dataset_code_scheme(dataset_id, code_scheme)

    def add_and_update_segment_code_schemes(self, segment_id, code_schemes, transaction=None):
        """
        Adds or updates code schemes for a given segment.

        :param segment_id: Id of the segment to add or update the code schemes for.
        :type segment_id: str
        :param code_schemes: Code schemes to be added or updated.
        :type code_schemes: list of core_data_modules.data_models.code_scheme.CodeScheme
        :param transaction: Transaction to run this in or None.
        :type transaction: google.cloud.firestore.Transaction | None
        """
        for code_scheme in code_schemes:
            self.set_segment_code_scheme(segment_id, code_scheme, transaction=transaction)

    def get_segment_messages_metrics_ref(self, segment_id):
        """
        Gets Firestore database reference to messages metrics.

        :param segment_id: Id of a segment.
        :type segment_id: str
        :return: A reference to messages metrics document in Firestore database
        :rtype: google.cloud.firestore.DocumentReference
        """
        return self._client.document(f"datasets/{segment_id}/metrics/messages")

    def get_segment_messages_metrics(self, segment_id, transaction=None):
        """
        Gets messages metrics for a given segment

        :param segment_id: Id of a segment.
        :type segment_id: str
        :param transaction: Transaction to run this get in.
        :type transaction: google.cloud.firestore.Transaction
        :return: Messages metrics for a given segment
        :rtype: core_data_modules.data_models.metrics.MessagesMetrics
        """
        messages_metrics = self.get_segment_messages_metrics_ref(segment_id).get(transaction=transaction).to_dict()
        if messages_metrics is None:
            return None
        return MessagesMetrics.from_firebase_map(messages_metrics)

    def set_segment_messages_metrics(self, segment_id, messages_metrics, transaction=None):
        """
        Sets messages metrics for a given segment

        :param segment_id: Id of a segment.
        :type segment_id: str
        :param messages_metrics: Messages metrics to set.
        :type messages_metrics: core_data_modules.data_models.metrics.MessagesMetrics
        :param transaction: Transaction to run this set in.
        :type transaction: google.cloud.firestore.Transaction | None
        """
        if transaction is None:
            self.get_segment_messages_metrics_ref(segment_id).set(messages_metrics.to_firebase_map())
        else:
            transaction.set(self.get_segment_messages_metrics_ref(segment_id), messages_metrics.to_firebase_map())

    @staticmethod
    def compute_message_metrics(message, code_schemes):
        """
        Computes the MessageMetrics for a single message.

        :param message: Message to compute the metrics for.
        :type message: core_data_modules.data_models.message.Message
        :param code_schemes: Code schemes that this message may have been labelled under.
        :type code_schemes: list of core_data_modules.data_models.code_scheme.CodeScheme
        :return: Message metrics for a single message.
        :rtype: core_data_modules.data_models.metrics.MessageMetrics
        """
        message_has_label = False
        message_has_ws = False
        message_has_nc = False

        code_schemes_lut = {code_scheme.scheme_id: code_scheme for code_scheme in code_schemes}

        for label in message.get_latest_labels():
            if not label.checked:
                continue

            message_has_label = True
            code_scheme_for_label = code_schemes_lut[label.scheme_id]
            code_for_label = None

            for code in code_scheme_for_label.codes:
                if label.code_id == code.code_id:
                    code_for_label = code

            assert code_for_label is not None
            if code_for_label.code_type == "Control":
                if code_for_label.control_code == "WS":
                    message_has_ws = True
                if code_for_label.control_code == "NC":
                    message_has_nc = True

        return MessagesMetrics(
            messages_count=1,
            messages_with_label=1 if message_has_label else 0,
            not_coded_messages=1 if message_has_nc else 0,
            wrong_scheme_messages=1 if message_has_ws else 0
        )

    def compute_segment_messages_metrics(self, segment_id, messages=None, transaction=None):
        """
        Compute and return the messages metrics for a given dataset.

        This method does not update Firestore.

        :param segment_id: Id of a segment.
        :type segment_id: str
        :param messages: list of core_data_modules.data_models.message.Message, defaults to None
                         If specified, it computes progress metrics based on the provided messages
                         else it downloads messages from the requested segment. Defaults to None.
        :type messages: core_data_modules.data_models.message.Message | None
        :param transaction: Transaction to run this get in.
        :type transaction: google.cloud.firestore.Transaction
        :return: Messages metrics.
        :rtype: core_data_modules.data_models.metrics.MessagesMetrics
        """
        if messages is None:
            messages = self.get_segment_messages(segment_id, transaction=transaction)

        if len(messages) == 0:
            return MessagesMetrics(0, 0, 0, 0)

        code_schemes = self.get_all_code_schemes(segment_id, transaction=transaction)

        segment_metrics = MessagesMetrics(0, 0, 0, 0)
        for message in messages:
            segment_metrics += CodaV2Client.compute_message_metrics(message, code_schemes)

        return segment_metrics

    def compute_and_update_dataset_messages_metrics(self, dataset_id):
        """
        Computes messages metrics of the given dataset.

        :param dataset_id: Id of the dataset to compute coding progress.
        :type dataset_id: str
        """
        segment_count = self.get_segment_count(dataset_id)
        for segment_index in range(1, segment_count + 1):
            segment_id = self.id_for_segment(dataset_id, segment_index)
            messages_metrics = self.compute_segment_messages_metrics(segment_id)
            self.set_segment_messages_metrics(segment_id, messages_metrics)

    def get_segment_ref(self, segment_id):
        """
        Gets Firestore database reference to a segment.

        :param segment_id: Id of a segment.
        :type segment_id: str
        :return: A reference to a document in a Firestore database
        :rtype: google.cloud.firestore.DocumentReference
        """
        return self._client.document(f"datasets/{segment_id}")

    def get_segment(self, segment_id, transaction=None):
        """
        Gets segment by id.

        :param segment_id: Id of a segment.
        :type segment_id: str
        :return: A snapshot of document data in a Firestore database.
        :rtype: google.cloud.firestore.DocumentSnapshot
        """
        return self.get_segment_ref(segment_id).get(transaction=transaction)

    def get_segment_user_ids(self, segment_id, transaction=None):
        """
        Gets user id in the given segment.

        :param segment_id: Id of a segment.
        :type segment_id: str
        :return: list of user ids.
        :rtype: list
        """
        return self.get_segment(segment_id, transaction=transaction).get("users")

    def ensure_user_ids_consistent(self, dataset_id, transaction=None):
        """
        Ensures user ids are consistent across all segments of the dataset.

        :param dataset_id: Id of a dataset.
        :type dataset_id: str
        :param transaction: Transaction to run this get in.
        :type transaction: google.cloud.firestore.Transaction
        """
        # Perform a consistency check on the other segments if they exist
        segment_count = self.get_segment_count(dataset_id)
        if segment_count == 1:
            return

        first_segment_users = self.get_segment_user_ids(dataset_id, transaction=transaction)
        for segment_index in range(2, segment_count + 1):
            segment_id = self.id_for_segment(dataset_id, segment_index)
            assert set(self.get_segment_user_ids(segment_id, transaction=transaction)) == set(first_segment_users), \
                f"Segment {segment_id} has different users to the first segment {dataset_id}"

    def get_dataset_user_ids(self, dataset_id, transaction=None):
        """
        Gets user ids for the given dataset.

        :param dataset_id: Id of a dataset.
        :type dataset_id: str
        :param transaction: Transaction to run this get in.
        :type transaction: google.cloud.firestore.Transaction
        :return: list of user ids.
        :rtype: list | None
        """
        self.ensure_user_ids_consistent(dataset_id, transaction=transaction)

        segment_snapshot = self.get_segment(dataset_id, transaction=transaction)
        if not segment_snapshot.exists:
            return None

        segment_doc = segment_snapshot.to_dict()
        if "users" not in segment_doc:
            return None
        return segment_doc["users"]

    def set_dataset_user_ids(self, dataset_id, user_ids):
        """
        Sets user ids for the given dataset.

        :param dataset_id: Id of a dataset.
        :type dataset_id: str
        :param user_ids: list of user ids.
        :type user_ids: list
        """
        segment_count = self.get_segment_count(dataset_id)
        batch = self._client.batch()

        for segment_index in range(1, segment_count + 1):
            segment_id = self.id_for_segment(dataset_id, segment_index)
            batch.set(self.get_segment_ref(segment_id), {"users": user_ids})

        batch.commit()
        log.debug(f"Wrote {len(user_ids)} users to dataset {dataset_id}")

    def set_segment_user_ids(self, segment_id, user_ids, transaction=None):
        """
        Sets user ids for the given segment.

        :param segment_id: Id of a segment to set user ids into.
        :type segment_id: str
        :param user_ids: list of user ids.
        :type user_ids: list
        :param transaction: Transaction to run this update in or None.
                            If None, adds the updates to a transaction that will then be explicitly committed.
        :type transaction: google.cloud.firestore.Transaction | None
        """
        if transaction is None:
            # If no transaction was given, run all the updates in a new batched-write transaction and flag that
            # this transaction needs to be committed before returning from this function.
            transaction = self._client.batch()
            commit_before_returning = True
        else:
            commit_before_returning = False

        transaction.set(self.get_segment_ref(segment_id), {"users": user_ids})

        if commit_before_returning:
            transaction.commit()

        log.debug(f"Wrote {len(user_ids)} users to dataset {segment_id}")

    def get_next_available_sequence_number(self, dataset_id, transaction=None):
        """
        Gets the sequence number of message being added to the given dataset.
        :param dataset_id: Id of a dataset.
        :type dataset_id: str
        :param transaction: Transaction to run this get in.
        :type transaction: google.cloud.firestore.Transaction
        :return: sequence number.
        :rtype: int
        """
        segment_count = self.get_segment_count(dataset_id, transaction=transaction)

        highest_seq_no = -1
        for segment_index in range(1, segment_count + 1):
            segment_id = self.id_for_segment(dataset_id, segment_index)
            messages_ref = self.get_messages_ref(segment_id)

            direction = firestore.Query.DESCENDING
            message_snapshots = messages_ref.order_by(
                "SequenceNumber", direction=direction).limit(1).get(transaction=transaction)

            if len(message_snapshots) == 0:
                continue

            [msg_snapshot] = message_snapshots
            message = Message.from_firebase_map(msg_snapshot.to_dict())
            if message.sequence_number > highest_seq_no:
                highest_seq_no = message.sequence_number

        return highest_seq_no + 1

    def add_message_to_dataset(self, dataset_id, message):
        """
        Adds message to a given dataset.

        :param dataset_id: Id of the dataset to add the message into.
        :type dataset_id: str
        :param message: The message to be added.
        :type message: core_data_modules.data_models.message.Message
        """
        message = message.copy()

        @firestore.transactional
        def add_in_transaction(transaction):
            message_id = message.message_id

            message_exists = self.get_dataset_message(dataset_id, message_id, transaction=transaction) is not None
            assert not message_exists, f"message with id {message_id} already exists."

            log.debug(f"Adding message with id {message_id} to Coda dataset {dataset_id}")

            segment_count = self.get_segment_count(dataset_id, transaction=transaction)
            latest_segment_id = self.id_for_segment(dataset_id, segment_count)

            message.last_updated = firestore.firestore.SERVER_TIMESTAMP
            message.sequence_number = self.get_next_available_sequence_number(dataset_id, transaction=transaction)
            message_metrics = self.compute_segment_messages_metrics(latest_segment_id, [message], transaction=transaction)  # nopep8

            segment_messages_metrics = self.get_segment_messages_metrics(latest_segment_id, transaction=transaction)
            if segment_messages_metrics is None:
                segment_messages_metrics = self.compute_segment_messages_metrics(latest_segment_id, transaction=transaction)  # nopep8

            latest_segment_size = segment_messages_metrics.messages_count
            if latest_segment_size >= self.MAX_SEGMENT_SIZE:
                # Any read operation after this will raise ReadAfterWriteError
                self.create_next_segment(dataset_id, transaction=transaction)
                latest_segment_id = self.id_for_segment(dataset_id, segment_count + 1)
                segment_messages_metrics = MessagesMetrics(0, 0, 0, 0)

            # Set message
            message_ref = self.get_message_ref(latest_segment_id, message_id)
            transaction.set(message_ref, message.to_firebase_map())

            # Set messages metrics
            updated_messages_metrics = segment_messages_metrics + message_metrics
            segment_messages_metrics_ref = self.get_segment_messages_metrics_ref(latest_segment_id)
            transaction.set(segment_messages_metrics_ref, updated_messages_metrics.to_firebase_map())

        add_in_transaction(self.transaction())
