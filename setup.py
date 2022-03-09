import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="databricks-migration-tool",
    version="1.0.1",
    author="Miklos C",
    author_email="mwc@databricks.com",
    description="Databricks Migration Tool",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/databrickslabs/migrate",
    license="https://github.com/databrickslabs/migrate/blob/master/LICENSE",
    packages=setuptools.find_packages(),
    install_requires=[
          'cron-descriptor',
          'mlflow-skinny',
          'sqlparse',
          'requests'
    ],
    py_modules=["export_db","import_db","test_connection","migration_pipeline"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
