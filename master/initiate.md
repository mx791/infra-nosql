# Initialisation du cluster MongoDb

Lancer toutes les intances sur chasque serveur, puis sur le serveur du noeud maitre, exécuter :

```
mongosh

rs.initiate({
   _id : "dbrs",
   members: [
      { _id: 0, host: "mongo1:27017" },
      { _id: 1, host: "mongo2:27017" },
      { _id: 2, host: "mongo3:27017" }
   ]
})
```
Le script est à adapter avec la liste des noeuds/adresse IP du cluster.