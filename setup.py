from setuptools import setup, find_packages

setup(
    name="sppd",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[*open("requirements.txt").read().splitlines()],
    author="Alvaro Carranza",
    author_email="alvarocarranzacarrion@gmail.com",
    description="Spanish Public Procurement Database tools and utilities",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/Alvaro2c/sppd",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.10",
)
