from setuptools import setup, find_packages
setup(
    name="RWE_Engine",
    version="0.1",
    packages=find_packages(),

    # Project uses reStructuredText, so ensure that the docutils get
    # installed or upgraded on the target machine
    install_requires=['docutils>=0.3'],
    include_package_date=True,
    zip_safe=False,


    # metadata to display on PyPI
    author="Rilwan Pade",
    author_email="rpade@celgene.com",
    description="This is a Package used to import data from various sources and run some generic qc logic",
    keywords="qc raw data import",
    url="http://example.com/HelloWorld/",   # project home page, if any
    project_urls={
        "Bug Tracker": "https://bugs.example.com/HelloWorld/",
        "Documentation": "https://docs.example.com/HelloWorld/",
        "Source Code": "https://code.example.com/HelloWorld/",
    }
)