from setuptools import setup

setup(
    name="CodaV2PythonClient",
    version="0.1.4",
    url="https://github.com/AfricasVoices/CodaV2PythonClient",
    packages=["coda_v2_python_client"],
    install_requires=["firebase_admin", "coredatamodules @ git+https://github.com/AfricasVoices/CoreDataModules"]
)
