import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="alert-pkg-mingyanisa",
    version="0.0.1",
    author="Yanisa Sunthornyotin",
    author_email="yanisasuntonyotin@gmail.com",
    description=" Alerts Automatic Triage",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/operationalintelligence/EmailAlertingSystem",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    packages=['pyspark',
                  'pyspark.mllib',
                  'pyspark.mllib.linalg',
                  'pyspark.mllib.stat',
                  'pyspark.ml',
                  'pyspark.ml.feature',
                  'pyspark.ml.clustering',
                  'pyspark.ml.linalg',
                  'pyspark.ml.param',
                  'pyspark.ml.param.shared',
                  'pyspark.sql',
                  'pyspark.sql.types',
                  'pyspark.sql.functions',
                  'pyspark.ml.pipeline',
                  'pyspark.streaming',
                  'pyspark.jars',
                  'pyspark.python.pyspark',
                  'datetime',
                  'numpy',
                  'math',
                  'scipy.spatial.distance',
                  'alertTriage_pkg.app'
                  ],
    install_requires=['py4j==0.10.8.1'],

)