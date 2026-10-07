# Runbook - exploiter et vérifier MobilityPulse France

Procédures détaillées. Présentation du projet : [README](../README.md). Conception et décisions :
[architecture.md](architecture.md).

## Prérequis

- Docker Desktop (WSL2) démarré, Compose v2.
- Python 3.11 pour les tests et le lint sur l'hôte (le projet exige `>=3.11,<3.12`).
- Accès Internet pour l'API Naolib (sinon : mode fixture, voir plus bas) et pour les tuiles
  OpenStreetMap de la carte Grafana (sans accès, le fond de carte reste vide mais les stations
  s'affichent).
- Ports hôte libres : **39092** (Kafka), **18080** (UI master), **18081** (UI worker), **14040** (UI
  driver Bronze), **14041** (UI Silver), **15432** (PostgreSQL), **13000** (Grafana). Tous sont
  configurables dans `.env`. Un `.env` ancien doit recevoir les nouvelles variables : comparer avec
  `.env.example`.

## Démarrage

```powershell
cp .env.example .env
```

**Avant le premier `docker compose`, remplacer dans `.env` les trois `change-me`**
(`POSTGRES_PASSWORD`, `GRAFANA_DB_PASSWORD`, `GRAFANA_ADMIN_PASSWORD`) par des mots de passe
aléatoires d'au moins une vingtaine de caractères (par ex. `openssl rand -base64 24`). Aucun secret
n'est stocké ailleurs que dans `.env` (ignoré par git). Docker Compose refuse de démarrer si l'un de
ces mots de passe est absent ou vide, **y compris pour les commandes qui n'en ont pas besoin** : le
fichier Compose est interprété en entier.

```powershell
docker compose up -d --build
docker compose ps
```

Premier build : téléchargement de l'image Python, de Java 17, de PySpark, de numpy et des JAR du
connecteur Kafka, donc plusieurs minutes. Environnement Python hôte (tests et lint) :

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
```

Onze services tournent en continu : `kafka`, `producer`, `spark-master`, `spark-worker`,
`bronze-job`, `silver-job`, `postgres`, `grafana`, `web` (API + interface, port 18000),
`live-updater` (met à jour l'état courant et les priorités) et `gold-refresher` (recalcule les
séries et taux horaires). `kafka-init` est un one-shot (code 0). Les jobs à la demande (profil
`tools`) : `compact-bronze`, `gold-job`, `loader`, `ml-job`, `rebalance-job`, `quality-lake`,
`quality`.

**Premier démarrage** : au lancement, `live-updater` applique les migrations (006 incluse) puis
rattrape les fichiers Silver récents (6 h au plus s'il n'y a rien en base). Les pages sont
disponibles dès que `web` est « healthy » :

| URL | Contenu |
|---|---|
| http://localhost:18000/ | Présentation du projet, chiffres en direct |
| http://localhost:18000/app | Application : carte, priorités, transferts, qualité |
| http://localhost:18000/api/docs | Documentation de l'API (OpenAPI) |
| http://localhost:18000/api/status | Fraîcheur des données et cause d'un retard (JSON) |

## Ce qui se met à jour tout seul, et ce qui ne le fait pas

| Quoi | Par quoi | Cadence |
|---|---|---|
| Collecte, Bronze, Silver | `producer`, `bronze-job`, `silver-job` | 60 s (poll), 30 s (déclencheurs) |
| `station_current` et priorités / transferts | `live-updater` | à chaque nouveau poll, vérifié toutes les `LIVE_INTERVAL_SECONDS` (10 s) |
| Séries 15 min et taux horaires | `gold-refresher` puis chargement par `live-updater` | `GOLD_REFRESH_SECONDS` (900 s) après la fin du calcul précédent |
| Push vers les navigateurs | `web` (`/api/stream`, SSE) | `STREAM_POLL_SECONDS` (2 s), seulement si un client écoute |
| Contrôles de qualité, ML, compaction | **à la main** | voir ci-dessous |

```powershell
docker compose run --rm --no-deps quality-lake    # contrôles du lac (Spark) -> data/quality/lake_latest.json
docker compose run --rm quality                   # contrôles de service + fusion du rapport du lac
docker compose run --rm --no-deps ml-job          # écrit data/ml/reports/latest.json
```

Le `loader` et `gold-job` à la demande restent utiles pour un rechargement complet (par exemple
après une reconstruction de Silver) : `docker compose run --rm loader` charge les trois tables ;
`docker compose run --rm loader python -m loader.main --skip-current` ne touche pas à
`station_current`, qui appartient à `live-updater`. Un chargement complet n'écrase pas l'historique
d'état (`first_observed_at`, `last_change_at`) : `live-updater` le relit en base au redémarrage.

## Commandes utiles

| Action | Commande |
|---|---|
| Logs producteur / Bronze / Silver | `docker compose logs -f producer` · `bronze-job` · `silver-job` |
| Redémarrer un job | `docker compose restart bronze-job` / `silver-job` |
| Journaux de la mise à jour en direct | `docker compose logs -f live-updater` (une ligne `Step` par nouveau poll, avec `elapsed_ms`) |
| Fraîcheur et cause d'un retard | `curl localhost:18000/api/status` |
| Arrêt propre (obligatoire, voir l'incident) | `docker compose stop` ; **jamais** d'arrêt brutal de Docker Desktop pendant que Spark tourne |
| Compaction Bronze (aperçu / réelle) | `docker compose run --rm --no-deps compact-bronze python -m streaming.compact_bronze --dry-run` / `docker compose run --rm --no-deps compact-bronze` |
| Gold en boucle (5 min) | `docker compose run --rm --no-deps gold-job python -m streaming.gold_job --every 300` |
| Gold à une date donnée (reproductible) | `docker compose run --rm --no-deps gold-job python -m streaming.gold_job --as-of 2026-10-06T16:00:00+00:00` |
| Migrations SQL seules | `docker compose run --rm loader python -m loader.main --migrate-only` |
| Charger seulement les N derniers jours | `docker compose run --rm loader python -m loader.main --days N` |
| Entraînement forcé (mécanique seulement) | `docker compose run --rm --no-deps ml-job python -m ml.train --force` : métriques marquées non fiables |
| Rééquilibrage, autre rayon / taille | `docker compose run --rm --no-deps rebalance-job python -m prescriptive.job --max-distance-km 2 --max-transfer 15` (`--force` ignore la garde de fraîcheur de 30 min) |
| Console SQL | `docker compose exec postgres psql -U mobilitypulse -d mobilitypulse` |
| Tout arrêter (données conservées) | `docker compose down` |
| Tout arrêter et vider Kafka | `docker compose down -v` (checkpoints et Parquet restent dans `./data`, à supprimer aussi pour repartir de zéro) |
| UI Spark master / worker / Bronze / Silver | http://localhost:18080 · :18081 · :14040 · :14041 |

## Procédure de vérification

1. **Services** : `docker compose ps` montre `kafka` (healthy), `producer`, `spark-master`,
   `spark-worker`, `bronze-job`, `silver-job`, `postgres` (healthy), `grafana`, `web` (healthy),
   `live-updater` et `gold-refresher` à l'état *Up*.
2. **Flux** : `docker compose logs producer --tail 5` affiche `Poll completed` avec `published` (105
   stations) toutes les 60 s ; `docker compose logs bronze-job --tail 5` affiche `Batch written`
   toutes les ~30 s.
3. **Données invalides** (depuis l'hôte, avec `.venv`) :

   ```powershell
   $env:KAFKA_BOOTSTRAP_SERVERS="localhost:39092"; $env:KAFKA_TOPIC="gbfs.station_status.v1"
   $env:GBFS_DISCOVERY_URL="x"; $env:GBFS_SOURCE_NAME="naolib-nantes"; $env:PYTHONPATH="src"
   .\.venv\Scripts\python.exe -m producer.inject_invalid
   ```

   Le job ne doit pas tomber ; un batch `bronze_rows: 0, rejected_rows: 7` apparaît.
4. **Inspection des Parquet** (schéma, comptages, doublons, raisons de rejet) :

   ```powershell
   docker compose run --rm --no-deps -T -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 `
     -e KAFKA_TOPIC=gbfs.station_status.v1 -e DATA_DIR=/data `
     bronze-job python -m streaming.inspect_lake
   ```

5. **Reprise après redémarrage** : relever `data/checkpoints/bronze_station_status/offsets/<dernier>`,
   faire `docker compose restart bronze-job`, vérifier que le batch suivant part des offsets du
   précédent (et non de 0), relancer l'inspection : `duplicated rows` reste à 0 après un arrêt
   propre.
6. **Silver** : un relevé par station et par poll ; son nombre de lignes égale celui de Bronze, au
   dernier poll près (Silver a jusqu'à environ une minute de retard). Comparaison exacte : chaque
   coordonnée Kafka de Silver est dans Bronze, et les lignes Bronze absentes de Silver sont toutes
   plus récentes que son dernier relevé. Redémarrer `silver-job` ne change pas ce nombre.
7. **Compaction** : `--dry-run` liste les partitions éligibles (jours clos uniquement). Le job
   vérifie le nombre de lignes avant de supprimer les petits fichiers.
8. **Gold** : `gold-refresher` le recalcule seul ; pour le vérifier, lancer `gold-job` puis le
   recalcul indépendant en Python pur depuis Silver :

   ```powershell
   docker compose run --rm --no-deps -T -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 `
     -e KAFKA_TOPIC=gbfs.station_status.v1 -e DATA_DIR=/data `
     gold-job python -m streaming.inspect_gold
   ```

   Neuf lignes `[PASS]`/`[FAIL]` (dernier relevé, dernier changement et âge de l'état par station ;
   nombre de créneaux ; `n_readings` / `n_changes` / dernier nombre de vélos par créneau ; bornes
   min / moyenne / max ; cohérence horaire ; taux dans [0, 1]). Code de sortie non nul en cas
   d'échec. Le contrôle se limite aux relevés que Gold a pu voir (Silver a de la latence).
9. **PostgreSQL** (après `loader`) : lignes lues, insérées, mises à jour, inchangées, supprimées
   journalisées. Un second `loader` sur le même Gold affiche `inserted: 0, updated: 0, deleted: 0`
   (idempotence). Le chargement des trois tables est atomique. `mobility.load_log` garde
   l'historique.
10. **Grafana** : cinq dashboards dans le dossier MobilityPulse (*Stations map*, *Critical
    stations*, *Station history*, *Rebalancing*, *Data quality*). Captures dans
    [img/](img/). Sur *Station history*, le panneau des taux horaires masque par défaut les heures
    dont moins de 95 % des stations ont été observées.
11. **Prédictif** : `ml-job` écrit `data/ml/reports/latest.json`. Lire `status` :
    `insufficient_data` (comptes et raisons, aucun modèle), `trained` (seuils de suffisance
    atteints), `trained_unreliable_forced` (avec `--force`, métriques à ne pas interpréter), ou
    `cannot_train`. Les métriques d'un modèle sont toujours accompagnées de la persistance
    (baseline) et des comptes d'échantillons.
12. **Rééquilibrage** : après `loader`, `rebalance-job`, puis contrôle par des requêtes SQL
    indépendantes : distances recalculées avec une formule haversine écrite en SQL, aucun donneur
    au-delà de son surplus, aucun receveur au-delà de son besoin, aucune station hors service
    utilisée, vélos entrants = sortants, classement sans inversion.
13. **Qualité des données** : `quality-lake` puis `quality`. Le code de sortie est non nul si un
    contrôle est en FAIL ; les WARN sont à lire (voir la section « Qualité des données » du README).
14. **Tests** :

    ```powershell
    .\.venv\Scripts\ruff.exe check .
    .\.venv\Scripts\ruff.exe format --check .
    .\.venv\Scripts\python.exe -m pytest -q -rs      # hôte : tests Spark et PostgreSQL ignorés
    docker compose run --rm --no-deps -T bronze-job python -m pytest -q -p no:cacheprovider /app/tests
    # Intégration contre le vrai PostgreSQL (un schéma jetable par test) :
    docker compose run --rm -T loader python -m pytest -q -rs -p no:cacheprovider /app/tests/integration /app/tests/unit/test_loader_pure.py /app/tests/unit/test_prescriptive.py /app/tests/unit/test_quality_core.py /app/tests/unit/test_live_state.py /app/tests/unit/test_live_tail.py /app/tests/unit/test_live_settings.py
    ```

    Les tests de l'API et du flux (`tests/integration/test_web_api.py`) et ceux des pages et du
    front (`test_web_pages.py`, `test_web_assets.py`, `test_web_js.py`) tournent sur l'hôte : il
    faut FastAPI, uvicorn et psycopg (`pip install -e ".[dev,loader,web]"`), `node` pour les scripts,
    et les variables `POSTGRES_*` de `.env` avec `POSTGRES_HOST=localhost` et `POSTGRES_PORT=15432`.

    Pour qu'un service absent fasse **échouer** les tests au lieu de les ignorer (comme en CI) :
    `CI_REQUIRE_SERVICES=1`.

## Publier la démo figée

La démo figée est un site statique : les pages lisent des fichiers JSON pris sur l'API à un instant
donné (voir le README). À refaire chaque fois que tu veux rafraîchir les chiffres.

1. **Photographier** (la pile tourne et contient des données) :

   ```powershell
   python scripts/export_demo.py
   ```

   Écrit `demo/data/` (105 stations, séries de 24 h, priorités, transferts, qualité, `meta.json` avec
   l'heure de la capture). Ce dossier **est commité** : la CI de publication n'a pas accès à ta pile.
2. **Construire** : `python scripts/build_demo.py [--repo-url https://github.com/<utilisateur>/<dépôt>]`
   écrit `demo-site/` (ignoré par git). Sans `--repo-url`, le bouton « Documentation de l'API » est
   masqué et aucun lien de code source n'est affiché.
3. **Tester sous un sous-chemin**, comme sur GitHub Pages :

   ```powershell
   mkdir test-hebergement; cp -r demo-site test-hebergement/<dépôt>
   python -m http.server 8123 --directory test-hebergement   # http://localhost:8123/<dépôt>/
   ```

   Vérifié le 7 octobre 2026 : 0 requête en échec, 0 erreur de console, navigation accueil ↔
   application, panneau d'une station avec son graphique, et aucune mention de « direct ».
4. **GitHub Pages** :
   - avant tout push, `git status` ne doit lister ni `.env` ni `data/` (ils sont dans `.gitignore`) ;
     un contrôle du 7 octobre 2026 n'a trouvé aucun des trois mots de passe dans les fichiers du projet ;
   - `git init`, `git add .`, `git commit`, créer le dépôt sur GitHub (public : Pages gratuit), `git push` ;
   - Réglages du dépôt → Pages → Source : **GitHub Actions**. Le workflow `pages.yml` construit et
     publie ; l'adresse est `https://<utilisateur>.github.io/<dépôt>/`.
5. **Render (site statique)** : New → Static Site, dépôt GitHub, *Build Command*
   `python scripts/build_demo.py`, *Publish Directory* `demo-site`. Le site est servi à la racine de
   son adresse.
6. **Mettre à jour** : relancer l'étape 1, commiter `demo/data`, pousser.

À vérifier de ton côté : la licence et l'obligation d'attribution des données GBFS de Naolib
(Nantes Métropole) pour un usage public ; la page d'accueil cite déjà la source. Le fond de carte
OpenStreetMap et la bibliothèque Leaflet (cdnjs) sont chargés depuis Internet.

## Vérifier le temps réel

1. **Fraîcheur** : `curl localhost:18000/api/status`. `freshness` vaut `live` (donnée de 150 s ou
   moins), `delayed` (jusqu'à 600 s) ou `stale` ; `cause` n'est pas nul dès que ce n'est pas `live`.
2. **Cause d'un retard** (le bandeau de l'application la reprend en français) :

   | `cause` | Sens | Où regarder |
   |---|---|---|
   | `collection_stopped` | le `live-updater` bat, mais plus de nouveau relevé | `producer` (API GBFS ?), `kafka`, `bronze-job`, `silver-job` |
   | `updater_down` | plus de battement depuis plus de 60 s | `docker compose logs live-updater`, `docker compose ps` |
   | `updater_error` | il bat mais échoue (erreur dans le dernier cycle) | `docker compose logs live-updater` |
   | `no_data` | aucune station en base | premier démarrage ? `loader`, `live-updater` |
   | `database_unavailable` | PostgreSQL injoignable | `docker compose ps postgres` ; l'API répond quand même |

   `gaps` liste les coupures de plus de 30 min sur 24 h (créneaux de 15 min manquants).
3. **Flux** : `curl -N localhost:18000/api/stream` doit afficher un `event: snapshot` immédiat puis
   un `event: update` à chaque nouveau poll (et un commentaire de maintien de connexion).
4. **Latence mesurée** : `python scripts/measure_sse_latency.py --seconds 1140 --out latence.jsonl`
   écoute le flux et écrit, pour chaque nouveau poll, le délai entre l'heure du poll et la réception
   (le premier message est un instantané, pas un échantillon). Tout tourne sur la même machine :
   pas de dérive d'horloge. Le rendu à l'écran n'est pas inclus (quelques millisecondes).
5. **Mémoire par service** : `docker stats --no-stream` (plusieurs échantillons, y compris pendant
   un calcul Gold de `gold-refresher`, le moment le plus gourmand).
6. **Panne simulée** : `docker compose stop postgres` : l'application affiche « Base de données
   indisponible », l'accueil masque ses chiffres ; `docker compose start postgres` : `live-updater`
   se reconnecte seul (reprise avec attente exponentielle, 30 s au plus).

## Mémoire de la VM Docker : constat du 7 octobre 2026

La VM Docker Desktop a 7,8 Gio. Avec la pile complète du projet (environ 2,5 Gio) et d'autres projets
qui tournent sur la même machine, il restait **environ 2,8 Gio disponibles et le swap était plein**
(2048 sur 2048 Mio, `docker run --rm --pid=host alpine free -m`). Conséquences constatées entre 20h05
et 20h17 UTC :

- un calcul Gold de `gold-refresher` a échoué avec `Cannot allocate memory` (`Gold run failed with
  exit code 1`) pendant qu'une suite de tests Spark tournait en même temps ; la boucle a continué et
  le cycle suivant (15 min plus tard) est le prochain essai, sans intervention ;
- la suite de tests Spark lancée **en un seul processus** (`pytest /app/tests`) a été tuée en cours de
  route (événement `oom` de Docker, `ConnectionRefusedError` vers la JVM) ;
- les mêmes tests, **fichier par fichier** (une JVM neuve chacun, comme en CI), passent tous (38 tests).

À faire : ne pas lancer de tests Spark pendant un calcul Gold, lancer la suite Spark par fichier, et
surveiller `free -m` dans la VM. Augmenter la mémoire de la VM (`.wslconfig`) ou arrêter les autres
projets règle le problème ; ne pas arrêter brutalement les conteneurs (voir l'incident ci-dessous).

```powershell
foreach ($f in "spark_validation","silver","gold","ml","quality_lake") {
  docker compose run --rm -T --no-deps bronze-job python -m pytest -q -p no:cacheprovider /app/tests/unit/test_$f.py
}
```

## Incident type : checkpoints corrompus par un arrêt brutal de Docker

Si Docker Desktop est fermé ou plante **sans** `docker compose stop`, des fichiers de checkpoint
Spark peuvent rester remplis de zéros (taille normale, contenu nul). Symptôme : `bronze-job` ou
`silver-job` redémarre en boucle avec `Log file was malformed: failed to read correct log version`
(`docker compose logs bronze-job`). Les données restent dans Kafka (rétention 7 jours) : rien n'est
perdu tant que le pipeline est relancé à temps. Constaté le 6 octobre 2026 : 81 minutes sans
collecte (17h41 → 19h02 UTC, producteur arrêté avec Docker).

Diagnostic et réparation (dans cet ordre, un service à la fois) :

1. `docker compose stop bronze-job silver-job`, puis chercher les fichiers entièrement nuls modifiés
   autour de l'arrêt dans `data/checkpoints/` et `data/*/_spark_metadata/`.
2. **Bronze** : si `commits/<N>` est nul, vérifier dans Bronze que le batch `N` est complet (chaque
   partition Kafka contient tous les offsets de `offsets/<N-1>` à `offsets/<N>`). Si oui, recopier
   le contenu de `commits/<N-1>` dans `commits/<N>` (et supprimer son `.crc`) : batch validé sans
   doublon. Sinon, supprimer `commits/<N>` : le batch est rejoué (doublons possibles, absorbés par
   Silver).
3. **Silver** (traitement avec état) : reconstruire depuis Bronze (supprimer
   `data/silver/station_status` et `data/checkpoints/silver_station_status`, relancer `silver-job`).
   Vérifier ensuite que chaque coordonnée Kafka de Silver est dans Bronze et inversement. Silver
   ne lit que `part-*` : une reconstruction ignorerait d'éventuels fichiers `compacted-*`.
4. Relancer `gold-job`, puis `loader --skip-current` (le miroir supprime ce que Gold ne contient
   plus ; `station_current` appartient au `live-updater`, qui relit les fichiers Silver reconstruits
   sans rien casser : ses mises à jour sont idempotentes), puis les contrôles de qualité.

Prévention : `docker compose stop` avant de quitter Docker Desktop. Le pipeline n'a pas d'alerte si
la collecte s'arrête (le contrôle `bronze_freshness_minutes` ne la voit que s'il est lancé).

## Mode fixture (hors-ligne)

Renseigner `GBFS_FIXTURE_DIR=tests/fixtures/gbfs` dans `.env`, puis `docker compose up -d producer`.
La fixture contient 12 stations figées (voir [tests/fixtures/gbfs/README.md](../tests/fixtures/gbfs/README.md)) :
les valeurs ne changent pas d'un poll à l'autre. Le producteur journalise `FIXTURE MODE`. Pour un
essai sans toucher à Bronze, publier vers un topic temporaire (`KAFKA_TOPIC` différent, créé à la
main : la création automatique est désactivée), puis le supprimer.

## Problèmes fréquents (Windows / WSL2)

- **Port déjà utilisé** (`bind: Only one usage of each socket address`) : une autre pile locale
  occupe le port. Ce projet évite volontairement 8080, 8081 et 29092. Changer `KAFKA_HOST_PORT`
  **et** `KAFKA_BOOTSTRAP_SERVERS` ensemble, ou les `*_PORT` voulus, dans `.env`. Voir les ports
  occupés : `docker ps --format "{{.Names}} {{.Ports}}"`.
- **Docker non démarré** (`error during connect`, `cannot find the file specified`) : lancer Docker
  Desktop et attendre que le moteur soit prêt (`docker version` doit afficher aussi la partie
  *Server*), puis `docker compose up -d`. Voir l'incident ci-dessus si un job redémarre en boucle.
- **Chemins MSYS (Git Bash)** : Git Bash réécrit les arguments qui ressemblent à des chemins POSIX
  (`/app/tests` devient `C:/Program Files/Git/app/tests`). Préfixer par `MSYS_NO_PATHCONV=1` ou
  utiliser PowerShell.
- **Producteur : `Failed to acquire idempotence PID … Coordinator load in progress`** au démarrage :
  transitoire (broker en initialisation), réessayé seul.
- **`required variable POSTGRES_PASSWORD is missing`** (ou `GRAFANA_*`) : renseigner les mots de
  passe dans `.env`.
- **Grafana : « password authentication failed »** : le rôle `grafana_ro` est créé par `loader` avec
  le mot de passe de `GRAFANA_DB_PASSWORD`. Lancer `docker compose run --rm loader python -m
  loader.main --migrate-only` après tout changement de ce mot de passe, puis redémarrer Grafana. Les
  volumes PostgreSQL et Grafana conservent leurs anciens mots de passe : changer `.env` ne suffit
  pas pour une instance déjà initialisée.
- **Grafana : panneaux « Loading plugin panel… » longtemps** : chargement initial lent des
  plugins ; attendre.
- **Variables manquantes sur l'hôte** : les scripts hôte lisent l'environnement, pas `.env`.
  Exporter les variables comme à l'étape 3.
- **Linux natif** : non testé. `./data` est monté dans le conteneur Spark (uid 1000) ; il faudra
  peut-être ajuster ses droits.
