# Infrastructure docker pour le projet NoSQL
Vous trouverez ici tous les fichiers nécessaires pour le déploiement de cluster MongoDB et Spark.
Les masters sont sur la même machine, toutes les autres machines hebergent quand à elles un "worker" mongo et un worker Spark.

Toute l'infrastructure se base sur Docker/Docker-compose, il faut donc que ces deux logiciels soient installés avant toute tentative de déploiement.
Pour tester votre installation, les commandes suivantes peuvent être utilisées :
```
docker version
docker run hello-world
docker-compose version
```

En cas d'echec d'une de ces commades, il faudra revoir l'installation...


## Installation des worker
Pour déployers les workers, cloner le dossier adéquat sur la machine.
Il faut ensuite mettre à jour le fichier .env, en y ajoutant l'adresse IP du noeud master. Celle-ci doit être accessible depuis l'intérieur des conteneurs, donc attention au DNS ect.
Cela fait, lancer la commande :
```
docker-compose up -d
```

## Installation du master
Cloner le dossier adéquat, puis lancer la commande :
```
docker-compose up -d
```
Lorsque tous les conteneurs auront démarré, il faudra configurer manuellement les réplicaSet MongoDB. Pour ce faire, accéder au shell dans le conteneur master mongo avec la commande 
```
docker-compose exec -it mongo bash
mongosh
```

Nous pouvons ensuite configurer la liste des réplicaSet, avec la commande :
```
rs.initiate({
   _id : "dbrs",
   members: [
      { _id: 0, host: "mongo1:27017" },
      { _id: 1, host: "mongo2:27017" },
      { _id: 2, host: "mongo3:27017" }
   ]
})
```
En adaptant bien évidemment les adresses des noeuds.