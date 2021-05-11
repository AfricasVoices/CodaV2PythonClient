import firebase_admin
from core_data_modules.logging import Logger
from firebase_admin import credentials
from firebase_admin import firestore


class FirebaseClientWrapper:
    def __init__(self, client):
        """Inits FirebaseClientWrapper

        Args:
            Firestore client (google.cloud.firestore.Firestore): Client for interacting with Google Cloud Firestore.
        """
        self._client = client

    @classmethod
    def init_client(cls, crypto_token_path, app_name):
        """Inits Firestore client

        Args:
            crypto_token_path (str): Path to the Firestore credentials file
            app_name (str): Name to call the Firestore app instance we'll use to connect.

        Returns:
            FirebaseClientWrapper: Firebase client
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
