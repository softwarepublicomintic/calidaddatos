#!/usr/bin/python

import requests
import json
from sodapy import Socrata
import pandas as pd
from calculating import *
import time
import sys
import logging


logging.basicConfig(filename= 'logs.txt', level = logging.INFO)


def getFirstID():

    #Esta funcion se encarga de identificar el total de conjuntos del dominio www.datos.gov.co y el id inicial

    listaIDs = []
    domains = requests.get("http://api.us.socrata.com/api/catalog/v1/domains", timeout=60)

    if domains.status_code == 200:


        #obteniendo numero de datasets

        domains = domains.json()
        results = domains['results']
        findDomain = [i['count'] for i in results if i['domain']=="www.datos.gov.co"]
        numberDataSet = findDomain[0]
        listaIDs.append(numberDataSet)

        firstID = requests.get("https://api.us.socrata.com/api/catalog/v1?domains=datos.gov.co&offset=1&limit=1", timeout=60)


        if firstID.status_code == 200:

            firstData = firstID.json()
            resultsIniciales = firstData["results"]
            primerDatSet = resultsIniciales[0]
            resource = primerDatSet["resource"]
            initialID = resource["id"]
            listaIDs.append(initialID)


    return listaIDs

#----------------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------

def getAllIds(firstID):

    #Esta función obtiene todos los IDS de los conjuntos de datos tupo "dataset"

    AllIDs = []
    limit = 1000

    numberDataSet = firstID[0]
    initialID = ""

    # Identifica la dimensión del ciclo
    numberDataSet = (numberDataSet / 1000) + 1
    numberDataSet = int(numberDataSet)


    for i in range(numberDataSet):


        secuenciaData = requests.get(
            "https://api.us.socrata.com/api/catalog/v1?domains=datos.gov.co&scroll_id={}&limit={}".format(initialID,limit), timeout=60)

        secuenceData = secuenciaData.json()

        resultsIniciales1 = secuenceData['results']
        longitud = len(resultsIniciales1)


        #Filtrando los tipos de datos tipo Dataset
        for j in range(longitud):

            primerDatSet1 = resultsIniciales1[j]
            resource1 = primerDatSet1["resource"]
            if resource1["type"]=="dataset":
                initialID = resource1["id"]
                AllIDs.append(initialID)



    return AllIDs

#----------------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------


def controllerCenter(allIDS):

    flagCounter = 0
    limit = 10

    token = 'sC4N6wXghMXaL2C3uUxVMphf0'
    client = Socrata('www.datos.gov.co',
                     token,
                     username="investigacionydesarrollo@grupokriterion.com.co",
                     password="Haritol77..")

    client.timeout = 180

    print("Conjuntos de datos a evaluar", len(allIDS))

    for i in allIDS:

        resultado = ""
        flagCounter += 1

        try:

            try:

                # Generando los insumos del proceso.

                datasetURL = "https://www.datos.gov.co/resource/{}.json".format(i)

                print(flagCounter)
                print(datasetURL)

                # Valida URL y si es accesible
                statusDataset = requests.get(datasetURL, timeout=60)

                # Obtiene información del Dataset
                datosDataSet = client.get(i, limit=limit)

                # Obteniendo Metadatos del Dataset
                metaDataset = client.get_metadata(i)

                # Reviando que el Dataset no esté vacio 
                frametoValidate = pd.DataFrame.from_records(datosDataSet)

                # Validando DataSet
                metaData = metaDataset['metadata']


            except KeyError as error:

                print(error)
                logging.error(str(error))
                indexCompletitud = 0
                indexCredibilidad = 0
                indexActualidad = 0
                indexTrazabilidad = 0
                indexDisponibildiad = 0
                indexConformidad = 0
                indexComprensibilidad = 0
                indexPortabilidad = 0
                indexConsistencia = 0
                indexExactitud = 0

            except TimeoutError as error:

                logging.error(str(error))

            except requests.exceptions.ConnectionError as error:

                logging.error(str(error))


            else:

                # Cálculo de indicadores para cada conjunto de datos evaluado

                if statusDataset.status_code == 200 and frametoValidate.empty == False:

                    # Se genera primer indicador de disponibilidad
                    resultado = str(i)

                    # Se genera primer indicador de disponibilidad
                    resultado = resultado + ';' + str(10)

                    # Creación de la Instancia
                    evaluation = Evaluation()

                    # Indicador Completitud
                    indexCompletitud = evaluation.indicadorCompletitud(frametoValidate)
                    resultado = resultado + ',' + str(indexCompletitud)

                    # Indicador Actualidad
                    indexActualidad = evaluation.indicadorActualidad(metaDataset, metaData)
                    resultado = resultado + ',' + str(indexActualidad)

                    # Indicador Credibilidad
                    indexCredibilidad = evaluation.indicadorCredibilidad(metaDataset)
                    resultado = resultado + ',' + str(indexCredibilidad)

                    # Indicador Trazabilidad
                    indexTrazabilidad = evaluation.indicadorTrazabilidad(metaDataset)
                    resultado = resultado + ',' + str(indexTrazabilidad)

                    # Indicador Conformidad
                    indexConformidad = evaluation.indicadorConformidad(metaDataset)
                    resultado = resultado + ',' + str(indexConformidad)

                    # Indicador Comprensibilidad
                    indexComprensibilidad = evaluation.indicadorComprensibilidad(metaDataset, frametoValidate)
                    resultado = resultado + ',' + str(indexComprensibilidad)

                    # Indicador Portabilidad
                    indexPortabilidad = evaluation.indicadorPortabilidad(datosDataSet)
                    resultado = resultado + ',' + str(indexPortabilidad)

                    # Indicador Consistencia
                    indexConsistencia = evaluation.indicadorConsisetencia(frametoValidate)
                    resultado = resultado + ',' + str(indexConsistencia)
                    # INdicador Exactitud
                    indexExactitud = evaluation.indicadorExactitud(metaDataset, frametoValidate)
                    resultado = resultado + ',' + str(indexExactitud)

                else:

                    print("error")

            finally:


                with open("Quality_Indicators.csv", 'a', encoding='UTF-8') as qIndicators:
                    qIndicators.write(str(resultado))
                    qIndicators.write('\n')

        except BrokenPipeError as errorBroken:

            logging.error(str(errorBroken))
            resultado = str(i) + "0,0,0,0,0,0,0,0,0,0"
            with open("Quality_Indicators.csv", 'a', encoding='UTF-8') as qIndicators:
                qIndicators.write(str(resultado))
                qIndicators.write('\n')




#----------------------------------------------------------------------------------------------------------------------


def main():

    result_getFirstID = getFirstID()
    result_getAllIds = getAllIds(result_getFirstID)
    controllerCenter(result_getAllIds)

main()