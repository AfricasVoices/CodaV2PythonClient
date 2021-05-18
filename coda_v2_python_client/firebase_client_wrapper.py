import firebase_admin
from core_data_modules.logging import Logger
from core_data_modules.data_models import Message
from firebase_admin import credentials
from firebase_admin import firestore

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
            message = self.get_segment_message(dataset_id, message_id)
        else:
            log.info(f"Checking for message with ID {message_id} in {segment_count} segments of {dataset_id}...")
            for segment_index in range(1, segment_count + 1):
                segment_id = self.id_for_segment(dataset_id, segment_index)
                message = self.get_segment_message(segment_id, message_id)
                if message is not None:
                    log.debug(f"Message found in segment {segment_id}")
                    break
                log.debug(f"Message not found in segment {segment_id}")
            else:
                log.debug(f"Message not found in Dataset {dataset_id}")
                return None

        return message
