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

    def get_segment_count(self, dataset_id):
        """
        Gets number of segments for a given dataset. If the dataset is not segmented, returns None

        :param dataset_id: Id of a dataset
        :type dataset_id: str
        :return: Number of segments for a given dataset
        :rtype: int | None
        """
        segment_count_doc = self._client.document(f"segment_counts/{dataset_id}").get().to_dict()
        if segment_count_doc is None:
            return None
        return segment_count_doc["segment_count"]

    def set_segment_count(self, dataset_id, segment_count):
        """
        Sets number of segments for a given dataset.

        :param dataset_id: Id of a dataset
        :type dataset_id: str
        :param segment_count: Number of segment for a given dataset.
        :type segment_count: int
        """
        self._client.document(f"segment_counts/{dataset_id}").set({"segment_count": segment_count})

    def get_message_ref(self, segment_id, message_id):
        """ 
        Gets Firestore database reference to a message.

        :param segment_id: Id of a segment
        :type segment_id: str
        :param message_id: Id of a message
        :type message_id: str
        :return: A reference to a document in a Firestore database
        :rtype: google.cloud.firestore_v1.document.DocumentReference
        """
        return self._client.document(f"datasets/{segment_id}/messages/{message_id}")

    def get_segment_message(self, segment_id, message_id):
        """
        Gets a message from a segment by id. If the message is not found, returns None.

        :param segment_id: Id of a segment.
        :type segment_id: str
        :param message_id: Id of a message.
        :type message_id: str
        :return: A message from a segment.
        :rtype: core_data_modules.data_models.message.Message | None
        """
        raw_message = self.get_message_ref(segment_id, message_id).get().to_dict()
        if raw_message is None:
            return None
        return Message.from_firebase_map(raw_message)

    def get_segment_messages(self, segment_id, last_updated_after=None, last_updated_before=None):
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
        :return: Messages in this segment, filtered by 'LastUpdated' timestamp if requested.
        :rtype: list of core_data_modules.data_models.message.Message
        """
        messages_ref = self._client.collection(f"datasets/{segment_id}/messages")
        if last_updated_after is not None:
            messages_ref = messages_ref.where("LastUpdated", ">", last_updated_after)
        if last_updated_before is not None:
            messages_ref = messages_ref.where("LastUpdated", "<=", last_updated_before)
        raw_messages = [message.to_dict() for message in messages_ref.get()]

        messages = []
        for message in raw_messages:
            if "LastUpdated" in message:
                message["LastUpdated"] = message["LastUpdated"].isoformat(timespec="microseconds")
            messages.append(message)

        return [Message.from_firebase_map(message) for message in messages]

    def get_message(self, dataset_id, message_id):
        """
        Gets a message from a dataset by id. If the message is not found, returns None.

        :param dataset_id: Id of a dataset.
        :type dataset_id: str
        :param message_id: Id of a message.
        :type message_id: str
        :return: A message from a dataset.
        :rtype: core_data_modules.data_models.message.Message | None
        """
        segment_count = self.get_segment_count(dataset_id)
        if segment_count is None or segment_count == 1:
            return self.get_segment_message(dataset_id, message_id)

        for segment_index in range(1, segment_count + 1):
            segment_id = self.id_for_segment(dataset_id, segment_index)
            message = self.get_segment_message(segment_id, message_id)
            if message is not None:
                log.debug(f"Message found in segment {segment_id}")
                return message

        return None

    def get_messages(self, dataset_id, last_updated_after=None):
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
        :rtype: list of dict
        """
        segment_count = self.get_segment_count(dataset_id)
        if segment_count is None or segment_count == 1:
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
                    msg = msg.to_dict()
                    if "LastUpdated" in msg:
                        if dataset_last_updated is None or msg["LastUpdated"] > dataset_last_updated:
                            dataset_last_updated = msg["LastUpdated"]
                        if dataset_first_updated is None or msg["LastUpdated"] < dataset_first_updated:
                            dataset_first_updated = msg["LastUpdated"]

            # Check all the segments for any messages between the latest one we fetched above and the most recently updated
            # message seen in any segment. If we didn't fetch any new messages for a segment, use the oldest timestamp
            # across all segments (dataset_first_updated) instead. This is to ensure we don't miss any messages that were
            # being labelled while we were pulling the separate segments, and is needed to maintain the consistency
            # guarantees we need for incremental fetch.
            for segment_id, segment_messages in messages_by_segment.items():
                segment_last_updated = None
                for msg in segment_messages:
                    msg = msg.to_dict()
                    if "LastUpdated" in msg:
                        if segment_last_updated is None or msg["LastUpdated"] > segment_last_updated:
                            segment_last_updated = msg["LastUpdated"]

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
        :rtype: google.cloud.firestore_v1.collection.CollectionReference
        """
        return self._client.collection(f"datasets/{segment_id}/code_schemes")

    def ensure_code_schemes_consistent(self, dataset_id):
        """
        Checks that the code schemes are the same in all segments

        :param dataset_id: Id of a dataset.
        :type dataset_id: str
        """
        segment_count = self.get_segment_count(dataset_id)
        if segment_count is None or segment_count == 1:
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

    def get_all_code_schemes(self, dataset_id):
        """
        Gets all code schemes for a given dataset

        :param dataset_id: Id of a dataset.
        :type dataset_id: str
        :return: Code schemes in this dataset
        :rtype: core_data_modules.data_models.code_scheme.CodeScheme
        """
        self.ensure_code_schemes_consistent(dataset_id)

        code_schemes = []
        for doc in self.get_code_schemes_ref(dataset_id).get():
            code_schemes.append(CodeScheme.from_firebase_map(doc.to_dict()))
        return code_schemes

    def get_segment_code_scheme_ref(self, segment_id, scheme_id):
        return self._client.document(f"datasets/{segment_id}/code_schemes/{scheme_id}")

    def set_code_scheme(self, dataset_id, code_scheme):
        scheme_id = code_scheme.scheme_id
        segment_count = self.get_segment_count(dataset_id)
        batch = self._client.batch()
        if segment_count is None or segment_count == 1:
            batch.set(self.get_segment_code_scheme_ref(dataset_id, scheme_id), code_scheme.to_firebase_map())
        else:
            for segment_index in range(1, segment_count + 1):
                segment_id = self.id_for_segment(dataset_id, segment_index)
                batch.set(self.get_segment_code_scheme_ref(segment_id, scheme_id), code_scheme.to_firebase_map())
        batch.commit()
        log.debug(f"Wrote scheme: {scheme_id}")

    def add_and_update_code_schemes(self, dataset_id, schemes):
        for scheme in schemes:
            self.set_code_scheme(dataset_id, scheme)

    def get_segment_messages_metrics_ref(self, segment_id):
        """
        Gets Firestore database reference to messages metrics.

        :param segment_id: Id of a segment.
        :type segment_id: str
        :return: A reference to messages metrics document in Firestore database
        :rtype: google.cloud.firestore_v1.document.DocumentReference
        """
        return self._client.document(f"datasets/{segment_id}/metrics/messages")

    def get_segment_messages_metrics(self, segment_id):
        """
        Gets messages metrics for a given segment

        :param segment_id: Id of a segment.
        :type segment_id: str
        :return: Messages metrics for a given segment
        :rtype: core_data_modules.data_models.metrics.MessagesMetrics
        """
        messages_metrics = self.get_segment_messages_metrics_ref(segment_id).get().to_dict()
        if messages_metrics is None:
            return None
        return MessagesMetrics.from_firebase_map(messages_metrics)

    def set_segment_messages_metrics(self, segment_id, messages_metrics):
        """
        Sets messages metrics for a given segment

        :param segment_id: Id of a segment.
        :type segment_id: str
        :param metrics_map: Messages metrics.
        :type metrics_map: core_data_modules.data_models.metrics.MessagesMetrics
        """
        self.get_segment_messages_metrics_ref(segment_id).set(messages_metrics.to_firebase_map())

    def compute_segment_coding_progress(self, segment_id, messages=None):
        """
        Compute and return the progress metrics for a given dataset.

        This method will initialise the counts in Firestore if they do not already exist.

        :param segment_id: Id of a segment.
        :type segment_id: str
        :param messages: list of core_data_modules.data_models.message.Message, defaults to None
        :param messages: If specified, it computes progress metrics based on the provided messages
                         else it downloads messages from the requested segment. Defaults to None.
        :type messages: core_data_modules.data_models.message.Message | None
        :return: Messages metrics.
        :rtype: core_data_modules.data_models.metrics.MessagesMetrics
        """
        if messages is None:
            messages = self.get_segment_messages(segment_id)

        messages_with_labels = 0
        wrong_scheme_messages = 0
        not_coded_messages = 0

        code_schemes = {code_scheme.scheme_id: code_scheme for code_scheme in self.get_all_code_schemes(segment_id)}

        for message in messages:
            # Test if the message has a label and if any of the latest labels are either WS or NC
            message_has_label = False
            message_has_ws = False
            message_has_nc = False

            for label in message.get_latest_labels():
                if not label.checked:
                    continue

                message_has_label = True
                code_scheme_for_label = code_schemes[label.scheme_id]
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

            # Update counts appropriately
            if message_has_label:
                messages_with_labels += 1
            if message_has_ws:
                wrong_scheme_messages += 1
            if message_has_nc:
                not_coded_messages += 1

        messages_metrics = MessagesMetrics(len(messages), messages_with_labels, wrong_scheme_messages, not_coded_messages)  # nopep8

        self.set_segment_messages_metrics(segment_id, messages_metrics)
        return messages_metrics

    def compute_coding_progress(self, dataset_id):
        """
        Computes coding progress of the given dataset.

        :param dataset_id: Id of the dataset to compute coding progress.
        :type dataset_id: str
        """
        segment_count = self.get_segment_count(dataset_id)
        if segment_count is None or segment_count == 1:
            self.compute_segment_coding_progress(dataset_id)
        else:
            for segment_index in range(1, segment_count + 1):
                segment_id = self.id_for_segment(dataset_id, segment_index)
                self.compute_segment_coding_progress(segment_id)

    def get_segment_ref(self, segment_id):
        """
        Gets Firestore database reference to a segment.

        :param segment_id: Id of a segment.
        :type segment_id: str
        :return: A reference to a document in a Firestore database
        :rtype: google.cloud.firestore_v1.document.DocumentReference
        """
        return self._client.document(f"datasets/{segment_id}")

    def get_segment(self, segment_id):
        """
        Gets segment by id.

        :param segment_id: Id of a segment.
        :type segment_id: str
        :return: A snapshot of document data in a Firestore database.
        :rtype: google.cloud.firestore_v1.base_document.DocumentSnapshot
        """
        return self.get_segment_ref(segment_id).get()

    def get_segment_user_ids(self, segment_id):
        """
        Gets user id in the given segment.

        :param segment_id: Id of a segment.
        :type segment_id: str
        :return: list of user ids.
        :rtype: list
        """
        return self.get_segment(segment_id).get("users")

    def ensure_user_ids_consistent(self, dataset_id):
        """
        Ensures user ids are consistent across all segments of the dataset.

        :param dataset_id: Id of a dataset.
        :type dataset_id: str
        """
        # Perform a consistency check on the other segments if they exist
        segment_count = self.get_segment_count(dataset_id)
        if segment_count is None or segment_count == 1:
            return

        first_segment_users = self.get_segment(dataset_id).get("users")
        for segment_index in range(2, segment_count + 1):
            segment_id = self.id_for_segment(dataset_id, segment_index)
            assert set(self.get_segment_user_ids(segment_id)) == set(first_segment_users), \
                f"Segment {segment_id} has different users to the first segment {dataset_id}"

    def get_user_ids(self, dataset_id):
        """
        Gets user ids for the given dataset.

        :param dataset_id: Id of a dataset.
        :type dataset_id: str
        :return: list of user ids.
        :rtype: list
        """
        self.ensure_user_ids_consistent(dataset_id)

        users = self.get_segment(dataset_id).get("users")
        return users

    def set_user_ids(self, dataset_id, user_ids):
        """
        Sets user ids for the given dataset.

        :param dataset_id: Id of a dataset.
        :type dataset_id: str
        :param user_ids: list of user ids.
        :type user_ids: list
        """
        segment_count = self.get_segment_count(dataset_id)
        batch = self._client.batch()
        if segment_count is None or segment_count == 1:
            batch.set(self.get_segment_ref(dataset_id), {"users": user_ids})
        else:
            for segment_index in range(1, segment_count + 1):
                segment_id = self.id_for_segment(dataset_id, segment_index)
                batch.set(self.get_segment_ref(segment_id), {"users": user_ids})
        batch.commit()
        log.debug(f"Wrote {len(user_ids)} users to dataset {dataset_id}")
