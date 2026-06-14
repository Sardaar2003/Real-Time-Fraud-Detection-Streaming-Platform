import setuptools

setuptools.setup(
    name='fraud_detection_dataflow',
    version='0.1.0',
    install_requires=[
        'jsonschema>=4.0.0',
        'google-cloud-aiplatform>=1.30.0',
    ],
    packages=setuptools.find_packages(),
)
