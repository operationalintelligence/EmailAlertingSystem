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
)