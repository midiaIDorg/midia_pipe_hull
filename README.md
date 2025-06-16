### midia_pipe_hull

This package controls the installation of midiaID novemberrain snakemake workflow.
For more recent pipelines, go to [https://github.com/midiaIDorg/midia_docker](midia_docker) github page.

## Pipeline installation:

Make sure your docker is configured (which includes the user being in the docker group on linux, see [https://docs.docker.com/engine/install/linux-postinstall](here)
To check it all works, run `docker run hello-world`.

Download the zip file [here](./dockerhub.zip).
Unzip it. 
Follow instructions in README from the unzipped folder.

## LICENSE

[License for `midia_pipe_hull`.](./LICENSE)

[Bruker's EULA for using 4DFF clustering algorithm.](./EULA_4DFF.pdf) [And licenses of dependencies of the 4DFF algorithm.](./BRUKER_THIRD-PARTY-LICENSE-README.txt)

The SAGE fork used in the pipeline is MIT-licensed, as mentioned in the prepared image in folders containing it at its different versions.
