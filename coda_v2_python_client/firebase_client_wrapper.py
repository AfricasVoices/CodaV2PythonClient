import firebase_admin
from core_data_modules.logging import Logger
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

    def get_dataset_ids():
        """
        Gets unique ids of datasets having included only one id for segmented dataset ids.

        :return: Unique ids of datasets having included only one id for segmented dataset ids.
        :rtype: set of str
        """
        segment_ids = get_segment_ids()
        assert len(segment_ids) == len(set(segment_ids)), "Segment ids not unique"

        dataset_ids = set(segment_ids)
        for dataset_id in get_segmented_dataset_ids():
            segment_count = get_segment_count(dataset_id)
            if segment_count is not None and segment_count > 1:
                for segment_index in range(2, segment_count + 1):
                    dataset_ids.remove(id_for_segment(dataset_id, segment_index))

        return dataset_ids

    def get_segment_ids():
        """
        Gets ids of all datasets including segmented dataset ids.

        :return: Ids of all datasets including segmented dataset ids.
        :rtype: list of str
        """
        ids = []
        for segment in self._client.collection(u'datasets').get():
            ids.append(segment.id)
        return ids

    def id_for_segment(dataset_id, segment_index=None):
        """
        Generates id for segmented dataset ids by concatinating dataset id & segment index.

        :param dataset_id: Id for a datasets including segemented datasets
        :type dataset_id: str
        :param segment_index: Index in the range of segment count starting from index two, defaults to None
        :type segment_index: int, optional
        :return: Output of concatinating dataset id & segment index
        :rtype: str
        """
        if segment_index is None or segment_index == 1:
            return dataset_id
        return dataset_id + f'_{segment_index}'

    def get_segmented_dataset_ids():
        """
        Gets segmented dataset ids 

        :return: Ids of all datasets that are segmented
        :rtype: list of str
        """
        segmented_dataset_ids = []
        for doc in self._client.collection("segment_counts").get():
            segmented_dataset_ids.append(doc.id)
        return segmented_dataset_ids
