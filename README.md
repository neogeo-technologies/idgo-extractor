# idgo-extractor
Extracteur de données géographiques de la plateforme IDGO

## Objectif de l’outil

- Exécute une tâche d’extraction (dans un contexte d’exécution géré par Celery)
- Réalise différents types d’extractions : raster/vecteur, sur des fichiers locaux, des bases de données locales

Le service d’extraction à proprement parler ne propose aucune IHM graphique. Il pourra ensuite être exploité par une ou des  applications web permettant aux utilisateurs de réaliser et gérer des commandes d’extraction via des IHM graphiques. Les spécifications de cette application et de ses IHM graphiques sortent du champs du présent document.

## Architecture

Architecture inspirée de l'extracteur de fichiers fonciers de geOrchestra :
https://github.com/neogeo-technologies/idgo-extractor

### Technologies utilisées
- Python 3: langage de programmation
- Flask: framework pour implémenter l’API web pour lancer une extraction
- Celery: bibliothèque Python utilisée pour gérer les tâches asynchrones
- Redis: serveur de messages
- GDAL/OGR: bibliothèque utilisée pour réaliser l’extraction à proprement parler

L’extracteur pourra être déployé directement sur un serveur virtualisé de l’infrastructure d’IDGO ou dans des containers Docker.

### Architecture du service d’extraction
Le service d’extraction est principalement composé des éléments suivants :
- API web (frontend) permettant d’instancier des demandes d’extraction (publisher ou producer)
- Serveur Redis (broker) en charge de gérer la pile de commandes d’extraction de manière asynchrone
- Service d’extraction (backend, consumer) en charge de lancer le traitement à proprement parler pour chaque commande d’extraction
- GDAL/OGR en charge de traiter les données


## Installation

### Docker

Dans un contexte Docker:
```
make docker-build
docker-compose up
```
L'API REST reste est alors disponible sur [http://localhost:8080/](http://localhost:8080/)

### Hors Docker

Hors Docker, sur une base Debian (Stretch)

```
sudo apt-get install -y \
        python3 \
        python3-all-dev \
        python3-pip \
        python3-gdal \
        python3-yaml \
        gdal-bin \
        redis-server

sudo pip3 install -r resources/requirements.txt
sudo pip3 install -r celery/requirements.txt
sudo pip3 install uwsgi 
```

Puis lancer le frontend et le backend (celery)
```
cd frontend; ./start.sh &; cd ..
cd celery; ./start.sh &; cd ..
```

S'assurer que le service Redis est lancé.

L'API REST est alors disponible sur localhost:5000

## API REST

### Opération : commander une extraction

Objectif : ajouter à la pile des commandes d’extraction une nouvelle commande.

Type de requête : HTTP(S) POST

Endpoint: /jobs

Body payload :

Document JSON
```
{
    "user_id": "Chaine de caractères identifiant l’utilisateur de manière unique",
    "user_name": "Chaine de caractères contenant le nom de famille de l’utilisateur",
    "user_first_name": "Chaine de caractères contenant le prénom de l’utilisateur",
    "user_company": "Chaine de caractères contenant le nom de l’organisme de l’utilisateur",
    "user_email_address": "Chaine de caractères contenant l’adresse email de l’utilisateur (indispensable pour l’avertir de l’avancement des traitements)",
    "user_address": "Chaine de caractères contenant l’adresse postale de l’utilisateur (indispensable pour lui envoyer un support physique avec les données)",
    "data_extractions": [
        {
            "dir_name": "Nom du répertoire dans lequel les données seront insérées",
            "source": "Chaine de connexion GDAL/OGR à la source de données sur laquelle porte l’extraction",
            "layer": "Couche à extraire (dans le cas de données vecteur) Cf. paramètre layer de orginfo : http://www.gdal.org/ogrinfo.html",
            "dst_format" :
            {
                "gdal_driver":            "Nom court du driver GDAL/OGR. Par exemple GTiff, ESRI Shapefile, GPKG",
                "extension":              'extension du fichier. Optionnel. Dans le cas où gdal_driver est "MapInfo file", doit être "tab" ou "mif"'
                "options": {
                    "nom_de_la_clef":       "valeur"
                },
                "layer_options":  # applicable uniquement pour les drivers vecteurs
                {
                    "nom_de_la_clef":       "valeur"
                }
            },
            "dst_srs": "Chaine de caractères contenant le système de coordonnées en sortie sous la forme d’un code EPSG. Exemple : 'epsg:2154' pour le Lambert93.",
            "footprint": "Emprise géographique d'extraction en WKT (chaine de caractère) ou GeoJSON (sous la forme d'un objet JSON)",
            "footprint_srs": "Chaine de caractères contenant le système de coordonnées de footprint. Si footprint est GeoJSON doit être EPSG:4326. Si footprint est WKT, peut être quelconque (y compris différent du SRS des données demandé en sortie",
            "img_overviewed": "Booléean JSon. Si ce paramètre est fixé à true, l'extracteur crée des overviews de manière itérative en appliquant un facteur d'échelle 2 depuis l'échelle nominale des données. Cette itération est stoppée dès que la taille d'un overview est strictement inférieure au paramètre img_overview_min_size (aucun overview dont une des dimensions est strictement inférieure à ce paramètre ne doit être créé).",
            "img_overview_min_size" : "Valeur entière. Dimension minimum des overviews intégrées au fichier image produit (les overviews sont créés jusqu'à ce que leur largeur et hauteur est inférieure à ce paramètre). Valeur par défaut: 256",
            "img_res": "Nombre flottant. Résolution de l’image produite dans l'unité de dst_srs (mètres ou degrés en fonction des cas). La même résolution est appliquée en x et y.",
            "img_resampling_method": "Chaine de caractères. Méthode de ré-échantillonage appliquée par GDAL. Supporte les valeurs proposées par GDAL: nearest, bilinear, cubic, cubicspline, lanczos, average"
        }
    ],
    "additional_files": [
        {
            "file_name": "Nom du fichier avec son extension",
            "dir_name": "Nom du répertoire dans lequel le fichier sera copié",
            "file_location": "URL vers le fichier à copier"
        }
    ],
    "extracts_volume": "Adresse du volume dans lequel les données extraites doivent être déposées",
    "compress_extract": "Booléen JSON. Indique si les données extraites doivent être placées dans une archive. Sinon un répertoire contenant les données est créé. Valeur par défaut : true. Un résultat non compressé ne sera pas disponible au téléchargement."
}
```

L'élément dst_format peut intégrer les options de création spécifique du driver GDAL utilisé.

Exemple 1 pour la création d'un fichier GeoTIFF encodé en JPEG, avec une compression de faible qualité, tuilé avec des tuiles de 256 pixels de côté :
```
    "dst_format": {
        "gdal_driver": "GTiff",
        "options": {
            "COMPRESS": "JPEG",
            "JPEG_QUALITY": 50,
            "TILED": "YES",
            "BLOCKXSIZE": 256,
            "BLOCKYSIZE": 256
        }
```

Exemple 2 pour la création d'un fichier GeoTIFF encodé en LZW, tuilé avec des tuiles de 512 pixels de côté et accompagné d'un fichier TFW :
```
    "dst_format": {
        "gdal_driver": "GTiff",
        "options": {
            "COMPRESS": "LZW",
            "TFW": "YES",
            "TILED": "YES",
            "BLOCKXSIZE": 512,
            "BLOCKYSIZE": 512
        }
```

Exemple 3 pour la création d'un fichier GeoJSON dont l'arborescence des attributs est supprimée, limitant la précision des coordonnées à 5 chiffres après la virgule, intégrant une BBOX et une description de la couche :
```
    "dst_format": {
        "gdal_driver": "GeoJSON",
        "options": {
            "FLATTEN_NESTED_ATTRIBUTES": "YES",
            "NESTED_ATTRIBUTE_SEPARATOR": "-"
        },
        "layer_options": {
            "WRITE_BBOX": "YES",
            "COORDINATE_PRECISION": 4,
            "DESCRIPTION": "This is just an example description"
        }
```


Réponse (HTTP status codes) :
201 - Réponse positive
400 - Dans le cas où les paramètres de la requête ne correspondent pas aux spécifications qui suivent
500 - Dans le cas d’une anomalie non anticipée

Réponse body:

Document JSON en cas de succès
```
{
    "query": "Reprise de la requête entrante sous forme de dictionnaire JSon",
    "datetime": "Chaine de caractère au format ISO-8601. Par ex: 2017-06-29T18:00:55Z",
    "possible_requests": {
        "status": {
            "url": "http://localhost:5000/jobs/{task_id}",
            "verb": "GET"
        },
        "abort": {
            "payload": {"status": "STOP_REQUESTED"},
            "url": "http://localhost:5000/jobs/{task_id}",
            "verb": "PUT"}
        },
    'status': 'SUBMITTED',
    'task_id': 'Identifiant unique sous forme de chaine de caractère. Par ex 5dbaab61-4321-4179-956e-b036a951f215'
}
```

Dans la partie `query` de la réponse, 2 subtilités :
* l'élément footprint est exprimé en WKT même si l'utilisateur l'a fourni en GeoJSON ;
* un élément footprint_geojson est ajouté et contient la conversion du footprint en GeoJSON 
(selon la système de coordonnées géographiques EPSG:4326).

Document JSON en cas d'échec à la validation 
```
{
    "status": "ERROR",
    "detail": "Raison détaillée de l'erreur",
    "incoming_post_data": "reprise de la payload d'entrée",
    "exception": "Optionnel. Texte associée à l'exception Python"
}
```

### Opération : consulter l’avancement d’une extraction

Objectif : consulter l’avancement d’une extraction

Type de requête : HTTP(S) GET

Endpoint: /jobs/{task_id}

Body payload : aucun

Réponse (HTTP status codes) :
200 - Cas normal : lorsque la commande d’extraction existe et qu’aucune anomalie n’est rencontrée
404 - Dans le cas où la tache n’existe pas
500 - Dans le cas d’une anomalie non anticipée

Réponse body:
Document JSON
```
{
    "task_id: "identifiant de la tache",
    "status": "SUBMITTED, STARTED, PROGRESS, SUCCESS, STOP_REQUESTED, STOPPED ou FAILED",
    "hostname": "nom du worker prenant en charge la tache. Présent pour status = STARTED ou PROGRESS",
    "pid": "identifiant du processus sur le worker prenant en charge la tache. Présent pour status = STARTED ou PROGRESS",
    "query": "reprise de la requête initiale passée à la commande d'extraction",
    "possible_requests": {
        "status": {
            "url": "http://localhost:5000/jobs/{task_id}",
            "verb": "GET"
        },
        "abort": {
            "payload": {"status": "STOP_REQUESTED"},
            "url": "http://localhost:5000/jobs/{task_id}",
            "verb": "PUT"}
        },
        "download" :  {  # uniquement présent pour status = SUCCESS
            "url": "http://localhost:5000/jobs/{task_id}/download",
            "verb': "GET"
        }
    },
    "error": "chaine de caractère contenant le message d'erreur.  Présent pour status = FAILED",
    "progress_pct": "nombre entre 0 et 100 avec le pourcentage 'avancement. Présent pour status = PROGRESS",
    "extract_location": "chemin vers le fichier ZIP ou le répertoire contenant le résultat de l'extraction. Présent pour status = SUCCESS"
}
```
Détail des états:
SUBMITTED: tache mise en queue mais non encore dispatchée ou démarrée
STARTED: tache en cours de démarrage par un worker (état transitoire très court)
PROGRESS: tache en cours de traitement, associée à un pourcentage d'avancement
SUCCESS: tache terminée avec succès (état final)
STOP_REQUESTED: tache dont l'annulation a été demandée
STOPPED: tache terminée suite à une demande d'annulation (état final)
FAILED: tache terminée suite à une erreur (état final)

#### Opération : stopper une extraction

Objectif : stopper une commande d’extraction.
La commande d’extraction est simplement retirée de la pile des extraction à traiter. Si le traitement avait déjà commencé à être traité, il est alors stoppé. Aucun résultat d’extraction ne sera disponible pour l’utilisateur. Il pourra néanmoins consulter l’avancement de sa commande. Son état sera alors "STOPPED"

Type de requête : HTTP(S) PUT

Endpoint: /jobs/{task_id}

Body payload :
```
{
    "status": "STOP_REQUESTED"
}
```

Réponse (HTTP status codes) :
201 - Cas normal : lorsque la commande d’extraction existe, qu’elle est dans la pile des traitements en attente ou en cours d’exécution et qu’aucune anomalie n’est rencontrée
400 - Dans le cas où les paramètres de la requête ne correspondent pas à ce qui est attendu
404 - Dans le cas où la tache n’existe pas
409 - Dans le cas où la commande d’extraction existe mais ne peut pas être arrêtée (par exemple lorsqu’elle a déjà été arrêtée ou qu’elle est terminée en échec ou avec succès)
500 - Dans le cas d’une anomalie non anticipée

Réponse body:
Document JSON
```
{
    "task_id: "identifiant de la tache",
    "status": "STOP_REQUESTED",
    "possible_requests": {
        "status": {
            "url": "http://localhost:5000/jobs/{task_id}",
            "verb": "GET"
        }
    }
}
```

### Opération : obtenir le résultat d'une commande d'extraction

Objectif : obtenir le résultat d'une commande d'extraction achevée avec succès,
sous forme d'attachement ZIP

Type de requête : HTTP(S) GET

Endpoint: /jobs/{task_id}/download

Body payload : aucun

Réponse (HTTP status codes) :
200 - Cas normal : lorsque la commande d’extraction existe et qu’aucune anomalie n’est rencontrée
404 - Dans le cas où la tache n’existe pas
409 - Dans le cas où la commande d’extraction existe mais n'est pas achevée avec succès
500 - Dans le cas d’une anomalie non anticipée

Réponse: attachement de Mime-Type application/zip


## Fichier de paramétrage du service

La variable d'environnement EXTRACT_SERVICE_CONF peut pointer vers un fichier de
configuration du service au format YAML.

Il peut comprendre les paramètres suivants, à fixer par l’administrateur du service d’extraction.

- celery_broker_url: Adresse du serveur de messages. Valeur par défaut: redis://localhost:6379
- celery_result_backend: Adresse du stockage des résultat des traitements. Valeur par défaut: redis://localhost:6379

## Tests

Le répertoire tests contient 2 scripts Python testant des conversions
de données vecteur et rasteur. Ils sont prévus pour fonctionner nominalement
avec la configuration "Hors Docker", avec au minimum le frontend lancé au prélable.
Le backend peut être lancé ultérieurement.

