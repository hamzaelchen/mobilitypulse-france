# Pitch d'entretien - MobilityPulse France

Tout ce qui suit est vérifiable dans le dépôt. Les chiffres datent du 6 octobre 2026. En cas de
doute, ouvrir le [README](../README.md) : il distingue ce qui est mesuré de ce qui ne l'est pas.

## La version de 30 secondes

« J'ai construit de bout en bout une plateforme de données sur les vélos en libre-service de Nantes :
je collecte l'état de 105 stations chaque minute dans Kafka, je le nettoie et l'agrège avec Spark
en couches Bronze, Silver et Gold, je le sers via PostgreSQL à cinq dashboards Grafana, et j'ai posé
des briques d'aide à la décision : des recommandations de rééquilibrage et une suite de contrôles de
qualité. Ce dont je suis le plus sûr, c'est la méthode : chaque couche est vérifiée par un recalcul
indépendant, et j'ai documenté mes erreurs, dont une qui avait faussé tout l'aval. »

## La version de 3 minutes

**Le problème.** Un opérateur veut savoir où il manque des vélos ou des bornes, et où déplacer des
vélos. Le flux public GBFS donne l'état instantané ; le conserver dans le temps ouvre la mesure, la
prévision et la recommandation.

**L'architecture.** Un producteur Python interroge l'API chaque minute et publie dans Kafka
(KRaft). Spark Structured Streaming écrit Bronze (tout, y compris les messages invalides avec leur
motif de rejet), puis Silver (rejeux dédoublonnés, colonnes dérivées). Un job batch calcule Gold
avec une heure de référence explicite, donc rejouable. Un chargeur Python fait des upserts
idempotents, dans une seule transaction, vers PostgreSQL, avec des migrations versionnées et
vérifiées par checksum. Grafana est entièrement provisionné par fichiers.

**Ce qui a fonctionné.** Silver est identique à Bronze dans les deux sens (22 680 lignes), Gold
passe neuf contrôles de recalcul indépendant, le chargement dans PostgreSQL est idempotent, et les
recommandations de rééquilibrage passent dix contrôles SQL indépendants (distances recalculées par
une formule haversine écrite en SQL, aucun donneur au-delà de son surplus, etc.).

**Ce qui s'est mal passé, et ce que j'en ai tiré.**
1. *Une erreur de fond.* Le champ `last_reported` du flux n'est pas rafraîchi quand les compteurs
   changent. Mon dédoublonnage Silver, fondé dessus, supprimait ~99 % des vrais changements : Gold
   et les dashboards montraient un réseau quasi figé, avec des stations « vides depuis 15 heures ».
   Je l'ai détecté en comparant avec Bronze (1016 changements de compteurs entre relevés
   consécutifs, dont 14 avec un nouveau `last_reported`), puis j'ai tout refait. Leçon : valider une
   hypothèse sur la donnée par une mesure avant de bâtir dessus.
2. *Un incident d'exploitation.* Un arrêt brutal de Docker a laissé des fichiers de checkpoint
   remplis de zéros ; la collecte a été coupée 81 minutes. Kafka avait gardé les données : j'ai
   vérifié que le dernier batch de Bronze était complet avant de le valider sans doublon, et
   reconstruit Silver depuis Bronze. Leçon : un médaillon qui conserve le brut rend les erreurs
   réparables.
3. *Un modèle prédictif que je n'ai pas pu livrer.* L'historique sain est trop court (quelques
   heures). Plutôt que d'annoncer un score, j'ai codé une garde de suffisance qui refuse
   d'entraîner sous des seuils explicites et publie les comptes. Les recommandations de
   rééquilibrage reposent donc sur une heuristique, étiquetée comme telle dans les tables et dans
   Grafana.

**Ce que je ferais ensuite.** Laisser tourner plusieurs jours pour avoir assez de données ; planifier
Gold, chargement, rééquilibrage et qualité ; ajouter une alerte quand la collecte s'arrête ;
valider l'utilité des transferts par une simulation ou des données d'exploitation ; exécuter la CI
sur GitHub.

## Questions probables, et des réponses fondées

**Pourquoi Kafka et pas un simple cron + fichiers ?** Parce que le découplage producteur / consommateur
a servi concrètement : après l'arrêt de Docker, les 1155 messages produits pendant que Bronze
était en panne étaient dans Kafka et ont été rattrapés sans perte. Je ne prétends pas que le volume (105 messages
par minute) l'exige : c'est un choix d'architecture, pas de dimensionnement.

**Exactly-once ou at-least-once ?** At-least-once. Bronze écrit un batch puis valide le checkpoint ; un
crash entre les deux pourrait doubler des lignes. Silver absorbe les rejeux par les coordonnées Kafka.
Je n'ai pas testé le crash en plein batch : je le dis.

**Pourquoi un watermark sur l'heure d'arrivée ?** Parce que l'heure d'événement du flux n'est pas
fiable (voir l'erreur n° 1). L'état du dédoublonnage reste borné (24 h) ; un doublon arrivant plus
tard serait conservé, c'est la limite assumée.

**Pourquoi Gold en batch et pas en streaming ?** Un agrégat sur 15 minutes avec une heure de
référence explicite est déterministe, rejouable et testable. Contrepartie : il relit tout Silver (environ 2 min),
acceptable à cette échelle, pas au-delà ; un process le relance toutes les 15 minutes, et l'état
courant, lui, passe par un chemin court (voir l'architecture).

**Comment savez-vous que vos agrégats sont justes ?** Un recalcul indépendant en Python pur depuis
Silver compare dernier relevé, dernier changement et comptes par créneau avec les Parquet de Gold.
En SQL, les distances sont recalculées par une autre formule. Ces contrôles ont trouvé un arrondi
au demi-pair (Python arrondit 10,5 à 10, SQL à 11) que les tests unitaires n'avaient pas vu.

**Vos recommandations de rééquilibrage sont-elles utiles ?** Je ne sais pas, et je ne le prétends pas.
Elles respectent leurs contraintes (surplus, besoin, rayon de 3 km, stations hors service exclues),
mais je n'ai ni vérité terrain ni simulation d'impact, les seuils sont des conventions, et
l'appariement est glouton, sans véhicules ni itinéraires.

**Pourquoi pas de modèle prédictif ?** Il faut des historiques de plusieurs jours : au moment de
l'exécution, j'avais 420 échantillons dont 0 pour l'entraînement (après des retards de 60 min, un
horizon de 30 min et un embargo temporel). Le découpage est chronologique et sans fuite d'information
(testé) ; la baseline de comparaison est la persistance.

**Comment gérez-vous les secrets ?** Uniquement par `.env` (ignoré par git). Compose refuse de démarrer
sans mot de passe ; un test vérifie que les fichiers de configuration versionnés (`.env.example`,
Compose, provisioning Grafana) n'en contiennent aucun. Grafana n'utilise qu'un
rôle PostgreSQL en lecture seule. En CI, le mot de passe de la base est généré à l'exécution et masqué.

**Votre CI tourne-t-elle ?** Oui : un premier run sur GitHub Actions (8 octobre 2026) a fait passer
ses 4 jobs (19 s à 81 s). Avant cela, je l'avais validée avec `actionlint` et rejoué ses commandes
localement, ce qui avait trouvé une erreur réelle (une liste de migrations oubliée dans un test).
Un seul run : je ne peux pas parler de sa stabilité dans la durée. Deux garde-fous évitent un vert trompeur : un service absent fait
échouer les tests au lieu de les ignorer, et un script échoue si trop de tests sont ignorés.

**Et la montée en charge ?** Elle n'est pas démontrée : un broker, Silver en `local[2]`, un chargeur
qui lit tout en mémoire. Les pistes (partitions, Silver sur le cluster, chargement incrémental)
sont identifiées, pas mesurées.

## Ce qu'il ne faut pas dire

- Que le projet « prédit » quoi que ce soit : il n'y a pas de modèle entraîné.
- Que les recommandations « améliorent le service » : non mesuré.
- Que c'est « prêt pour la production » : mono-nœud, qualité/ML/compaction manuels, aucune alerte
  active, une seule exécution de la CI sur GitHub.
- Qu'il y a « exactly-once » : c'est at-least-once, avec déduplication en aval.
- Des chiffres de performance au-delà de ce qui a été mesuré : une fenêtre de 19 minutes (délai du
  poll au navigateur : 41 à 104 s, médiane 77 s) et un instantané de mémoire. Pas de benchmark, pas
  de mesure d'images par seconde. Ne pas parler de « temps réel strict ».
