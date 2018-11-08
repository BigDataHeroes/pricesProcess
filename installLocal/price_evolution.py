#!/usr/bin/python

#imports
import io
import pandas as pd
import os
import sys
from hdfs3 import HDFileSystem

reload(sys)
sys.setdefaultencoding('utf-8')
inputF = sys.argv[1]
outputF = sys.argv[2]

hdfs = HDFileSystem(host='sandbox-hdp.hortonworks.com', port=8020)

# Metodos necesarios
def LimpiarBarrio(pBarrio):
  if (pBarrio[0].isdigit()):
    return pBarrio[4:]
  else:
    return pBarrio



#Visualizamos que tenemos
with hdfs.open(inputF) as f:
    df = pd.read_csv(f,header=[0, 1],delimiter=';',nrows=22)

#corregimos los nombres de las columnas
as_list = df.columns.tolist() 

as_list[0] = ''
as_list[1] = ("2010", "Primer trimestre")
as_list[2] = ("2010", "Segundo trimestre")
as_list[3] = ("2010", "Tercer trimestre")
as_list[4] = ("2010", "Cuarto trimestre")

as_list[5] = ("2011", "Primer trimestre")
as_list[6] = ("2011", "Segundo trimestre")
as_list[7] = ("2011", "Tercer trimestre")
as_list[8] = ("2011", "Cuarto trimestre")

as_list[9] = ("2012", "Primer trimestre")
as_list[10] = ("2012", "Segundo trimestre")
as_list[11] = ("2012", "Tercer trimestre")
as_list[12] = ("2012", "Cuarto trimestre")

as_list[13] = ("2013", "Primer trimestre")
as_list[14] = ("2013", "Segundo trimestre")
as_list[15] = ("2013", "Tercer trimestre")
as_list[16] = ("2013", "Cuarto trimestre")

as_list[17] = ("2014", "Primer trimestre")
as_list[18] = ("2014", "Segundo trimestre")
as_list[19] = ("2014", "Tercer trimestre")
as_list[20] = ("2014", "Cuarto trimestre")

as_list[21] = ("2015", "Primer trimestre")
as_list[22] = ("2015", "Segundo trimestre")
as_list[23] = ("2015", "Tercer trimestre")
as_list[24] = ("2015", "Cuarto trimestre")

as_list[25] = ("2016", "Primer trimestre")
as_list[26] = ("2016", "Segundo trimestre")
as_list[27] = ("2016", "Tercer trimestre")
as_list[28] = ("2016", "Cuarto trimestre")

as_list[29] = ("2017", "Primer trimestre")
as_list[30] = ("2017", "Segundo trimestre")
as_list[31] = ("2017", "Tercer trimestre")
as_list[32] = ("2017", "Cuarto trimestre")

as_list[33] = ("2018", "Primer trimestre")
as_list[34] = ("2018", "Segundo trimestre")
as_list[35] = ("2018", "Tercer trimestre")
as_list[36] = ("2018", "Cuarto trimestre")

df.columns = as_list

df

# generamos un .csv que sea facil de entender para el Tableau
mCsv="Barrio;Anyo;Trimestre;Precio" +os.linesep

#generamos el texto que tendra el -csv a la vez que limpiamos los datos
for row in df.iterrows():
  vBarrio = row[1][0]
  vAnyo=2010
  cContadorTrimestre=1
  vTrimestre="T"+str(cContadorTrimestre)
  cContadorTrimestre=1
  for vValor in row[1][1:]:
    
    mCsv = mCsv +LimpiarBarrio(vBarrio)+";"+str(vAnyo)+";"+vTrimestre+";"+vValor+os.linesep
    cContadorTrimestre = cContadorTrimestre + 1
    vTrimestre="T"+str(cContadorTrimestre)
    if (vTrimestre=="T5"):
      cContadorTrimestre=1
      vTrimestre="T"+str(cContadorTrimestre)
      vAnyo = vAnyo + 1

#Guardamos el .csv en el disco
with hdfs.open(inputF) as f:
    f.write(mCsv)
## Python will convert \n to os.linesep
f.close()



