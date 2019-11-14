# Data Science Course at SDU

The course/repo is run/maintained by Jakob Hviid <jah@mmmi.sdu.dk> and Fisayo Caleb Sangogboye <fsan@mmmi.sdu.dk>.

## Setup

In the root directory run the following from an administrative terminal:

```bash
docker-compose up -d
addroute.cmd
```

also, add a file to the HDFS setup by attaching to the namenode and running:

```bash
apt update
apt install wget
wget -O alice.txt https://www.gutenberg.org/files/11/11-0.txt
hdfs dfs -put alice.txt /
```

A sample of how to connect to spark is provided in example.py, which currently reads the alice.txt file and makes a word count.

`Important! Currently pyspark requires that it is run with python version 3.7.5 or lower so if you have python 3.8 installed it will not work.`
[See this issue for more info](https://github.com/pyinstaller/pyinstaller/issues/4265)

## Running pyspark code inside a container

To run spark code inside a container, an example was created in the `pysparkExampleImage` folder. The image can be created and deployed using the run.cmd command (needs to be run from inside the folder itself).
Change the python file as needed, and change the dockerfile to fit with your needs. For example, add python packages inside this file.
The first time it runs, it will take several minutes to complete. Subsequent runs should be ready within a second or two.

Note, to make this work, the container is attached to the "hadoop" network that is created by the docker-compose file. Also, the docker-compose file has been changed since the initial setup, which means it will have to be updated if you are running your own version. The changed components are only related to the network section of the file (added name) and the docker-compose version (changed to 3.5).

## Docker compose cheat sheet

Take a look [here](https://gabrieltanner.org/blog/docker-compose).

## Credits

This repo consists of several components created by Data Science Europe, but has been restructured into a part of a course run at the University of Southern Denmark.
The repositories are as follows:

- <https://github.com/big-data-europe/docker-hadoop>
- <https://github.com/big-data-europe/docker-spark>

To see more about how the images that are used in this course are constructed, please visit these repositories and explore the DockerFiles in the corresponding directories.
