# Requête SQL et écriture des résultats dans un fichier csv


import mysql.connector
from mysql.connector import errorcode
import os

# Crée (s'il n'existe pas déjà) et ouvre le fichier offres_cp.csv dans le dossier _resultats
target = open("_resultats/offres_cp.csv", 'aw')

# Ecrit le header des colonnes
target.write("CD_POST, LB_COMM, x, y, ID_OFFR, Nb_Total")
try:

	# Connexion au serveur
	cnx = mysql.connector.connect(user='root', password='root',
                              host='localhost', port='8889',
                              database='GDF')
	cursor = cnx.cursor()
	
	# Expression de la requête
	query = ("SELECT CD_POST, LB_COMM, x, y, ID_OFFR, count(*) as 'Nb_Total' FROM requete_4 R4, requete_6 R6, population P WHERE R6.CD_POST = P.PostalCode AND R6.LB_COMM = P.place AND R4.id_pers = R6.ID_PERS GROUP BY CD_POST, ID_OFFR ORDER BY Nb_Total")
	cursor.execute(query)
	
	for (CD_POST, LB_COMM, x, y, ID_OFFR, Nb_Total) in cursor:
		# Ecriture de chaque ligne dans le fichier
		target.write("%s,%s,%s,%s,%s,%s\n" % (CD_POST, LB_COMM, x, y, ID_OFFR, Nb_Total))
  		
	cursor.close()

except mysql.connector.Error as err:
  if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
    print("Something is wrong with your user name or password")
  elif err.errno == errorcode.ER_BAD_DB_ERROR:
    print("Database does not exists")
  else:
    print(err)
else:
  cnx.close()
  target.close()
