import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="databricks-migration", # Replace with your own username
    version="0.0.2",
    author="Miklos C",
    author_email="mwc@databricks.com",
    description="Databricks Migration scripts",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/mrchristine/db-migration",
    license="http://www.apache.org/licenses/LICENSE-2.0",
    packages=setuptools.find_packages(),
    install_requires=[
          'cron-descriptor',
          'requests'
      ],
    py_modules=["export_db","import_db","test_connection"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
