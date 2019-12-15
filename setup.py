
import setuptools

setuptools.setup(
    name="apache_logs",
    version='0.0.1.dev0',
    author="Philippe Tap",
    author_email="author@example.com",
    description="DAP project - apache logs pipelines",
    url="https://github.com/philtap/Apache_logs.git",
    license='MIT',
    packages=setuptools.find_packages(),
    install_requires=[
      'psycopg2>=2.8.4',
      'pymongo>=3.10.0',
      'azure-cosmos>=3.1.2',
      'dagster>=0.6.6',
      'dagit>=0.6.6',
      'dagster_pandas>=0.6.6',
      'pandas>=0.25.3',
      'plotly>=4.3.0',
      'psutil>=5.6.7',
      'Menu>=3.2.2',
    ],
    dependency_links=[
        'git+https://github.com/ib-da-ncirl/db_toolkit.git#egg=db_toolkit',
        'git+https://github.com/ib-da-ncirl/dagster_toolkit.git#egg=dagster_toolkit',
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)