import json

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from core_data_modules.logging import Logger
from core_data_modules.data_models import Message
from core_data_modules.data_models import CodeScheme

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

    def get_code_schemes_ref(self, dataset_id):
        return self._client.collection(f"datasets/{dataset_id}/code_schemes")
