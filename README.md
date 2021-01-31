# PostgreSQL anatomy extension
This is a prototype for a PostgreSQL extension that implements the Anatomy privacy algorithm along with other helpful functions like dataset analyser and data supresser.

It's not very efficient for big datasets as it's written with Pl/Python and most functions read the dataset to memory. To make it practical, it should be rewritten in Pl/pgSQL or C.

At the time being this extension is most useful for learning how Anatomy anonymization technique works by playing around with the dataset analyzer or debugging the algorithm step-by-step.

## Setup and usage is described in readme.txt


## Example: anatomy with 3 - diversity

### Initial table
![full](https://i.postimg.cc/qBbdZYXQ/Screenshot-2020-11-19-at-15-33-34.png)

### QI table
![qi](https://i.postimg.cc/VNBx9Hkq/Screenshot-2020-11-19-at-15-34-55.png)

### SA table
![sa](https://i.postimg.cc/Lspd1M24/Screenshot-2020-11-19-at-15-35-10.png)
