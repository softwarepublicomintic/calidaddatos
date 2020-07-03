import pandas as pd
import numpy as np
import json
import re
import logging
from datetime import datetime, timedelta


logging.basicConfig(filename= 'logs.txt', level = logging.INFO)

class Evaluation:

    def __init__(self):
        pass

    #-------------------------------------------------------------------------------------------------------------

    def indicadorCompletitud(self, dataset):


        try:

            totalCells = np.product(dataset.shape)
            missing_value = pd.DataFrame(dataset.isnull().sum(), columns=['Valores Faltantes'])
            missingColumnas = missing_value['Valores Faltantes'].sum()
            indexCompletitud = int((1 - (missingColumnas / totalCells)) * 10)
            return indexCompletitud

        except ZeroDivisionError as error:

            print("Error Completitud: ", i)
            logging.error('Indicador Completitud -' + str(error))
            indexCompletitud = 0
            return indexCompletitud

    # -------------------------------------------------------------------------------------------------------------

    def indicadorActualidad(self, results, metaData):

        # Obteniendo ultima fecha de actualizacion
        try:

            dias = 0

            rowsUpdatedAt = results['rowsUpdatedAt']
            rowsUpdatedAt = datetime.fromtimestamp(rowsUpdatedAt).strftime('%Y-%m-%d %I:%M:%S')
            rowsUpdatedAt = datetime.strptime(rowsUpdatedAt, '%Y-%m-%d %I:%M:%S')

            # obteneiendo frecuencia de actualizacion

            customDatos = metaData['custom_fields']
            infoDatos = customDatos['Información de Datos']
            dateUpdate = infoDatos['Frecuencia de Actualización']
            # dateUpdate = datetime.strptime(dateUpdate, '%Y-%m-%d %I:%M:%S')

            if dateUpdate == 'Diaria' or dateUpdate == 'No aplica':
                dias = 1
            elif dateUpdate == 'Semanal':
                dias = 7
            elif dateUpdate == 'Quincenal':
                dias = 15
            elif dateUpdate == 'Mensual':
                dias = 30
            elif dateUpdate == 'Trimestral':
                dias = 90
            elif dateUpdate == 'Semestral':
                dias = 183
            elif dateUpdate == 'Anual':
                dias = 365

            # Fecha de Hoy menos dias de actualizacion
            todayDate = datetime.now()
            targetDateUpdate = datetime.now() - timedelta(days=dias)

            # Indicador Actualidad: si False no cumple - si True si cumple
            indexActualidad = targetDateUpdate < rowsUpdatedAt
            if indexActualidad == False:
                indexActualidad = 0
            else:
                indexActualidad = 10

        except KeyError as error:

            logging.error('Indicador Actualidad -' + str(error))
            indexActualidad = 0

        except TypeError as error:

            logging.error('Indicador Actualidad -' + str(error))
            indexActualidad = 0

        indexActualidad = int(indexActualidad)
        return indexActualidad

    # -------------------------------------------------------------------------------------------------------------

    def indicadorCredibilidad(self, results):

        try:

            # Obteneiendo informacion asociada a credibilidad
            approvals = results['approvals']
            datos = approvals[0]
            submitter = datos['submitter']

            displayName = submitter['displayName']

            if displayName is not None:
                indexCredibilidad = 5
            else:
                indexCredibilidad = 0

            indexCredibilidad = int(indexCredibilidad)


        except KeyError as error:

            logging.error('Indicador Credibilidad -' + str(error))
            indexCredibilidad = int(0)

        try:

            email = submitter['email']

            if email is not None:
                indexCredibilidad = indexCredibilidad + 5
            else:
                indexCredibilidad = indexCredibilidad + 0

            indexCredibilidad = int(indexCredibilidad)

        except KeyError as error:

            logging.error('Indicador Credibilidad -' + str(error))
            indexCredibilidad = indexCredibilidad + 0

        return indexCredibilidad


    # -------------------------------------------------------------------------------------------------------------

    def indicadorTrazabilidad(self, results):


        try:

            publicationDate = results['publicationDate']
            creationDate = results['createdAt']
            metaData = results['metadata']
            customDatos = metaData['custom_fields']
            infoDatos = customDatos['Información de Datos']
            dateUpdate = infoDatos['Frecuencia de Actualización']

            if publicationDate is not None and creationDate is not None and dateUpdate is not None:
                indexTrazabilidad = 10

            else:
                indexTrazabilidad = 0


            return indexTrazabilidad

        except KeyError as error:

            logging.error('Indicador Trazabilidad - ' + str(error))
            indexTrazabilidad = 0
            return indexTrazabilidad

    # -------------------------------------------------------------------------------------------------------------

    def indicadorConformidad(selfm, results):

        indexConformidad = 0
        vectorConformidad = []


        try:

            if results['name'] is not None:
                vectorConformidad.append(10)
                nombre = results['name']
            else:
                vectorConformidad.append(0)
                nombre = ""

        except KeyError as error:

            logging.error('Indicador Conformidad -' + str(error))
            vectorConformidad.append(0)
            nombre = ""

        try:

            if results['description'] is not None:
                vectorConformidad.append(10)

            else:
                vectorConformidad.append(0)
                descripcion = ""

        except KeyError as error:

            logging.error('Indicador Conformidad -' + str(error))
            vectorConformidad.append(0)
            descripcion = ""

        try:

            if results['category'] is not None:
                vectorConformidad.append(10)
                categoria = results['category']
            else:
                vectorConformidad.append(0)
                categoria = ""

        except KeyError as error:

            logging.error('Indicador Conformidad -' + str(error))
            vectorConformidad.append(0)
            categoria = ""

        try:

            if results['rowsUpdatedAt'] is not None:
                vectorConformidad.append(10)
            else:
                vectorConformidad.append(0)

        except KeyError as error:

            logging.error('Indicador Conformidad -' + str(error))
            vectorConformidad.append(0)

        try:

            approvals = results['approvals']
            datos = approvals[0]
            submitter = datos['submitter']
            email = submitter['email']

            if email is not None:
                vectorConformidad.append(10)
            else:
                vectorConformidad.append(0)

        except KeyError as error:

            logging.error('Indicador Conformidad -' + str(error))
            vectorConformidad.append(0)

        try:

            if results['attribution'] is not None:
                vectorConformidad.append(10)
                atribucion = results['attribution']
            else:
                vectorConformidad.append(0)
                atribucion = ""

        except KeyError as error:

            logging.error('Indicador Conformidad -' + str(error))
            vectorConformidad.append(0)
            atribucion = ""

        try:

            if results['id'] is not None:
                vectorConformidad.append(10)
            else:
                vectorConformidad.append(0)

        except KeyError as error:

            logging.error('Indicador Conformidad -' + str(error))
            vectorConformidad.append(0)

        longVectorConformidad = len(vectorConformidad)

        for m in vectorConformidad:
            indexConformidad = indexConformidad + m

        indexConformidad = round(indexConformidad / longVectorConformidad)
        indexConformidad = int(indexConformidad)

        return indexConformidad

    # -------------------------------------------------------------------------------------------------------------

    def indicadorComprensibilidad(self, results, dataframe):

        nombresMetaDatos, nombresDatos = [], []
        columnas = results['columns']
        columns = pd.DataFrame(list(dataframe.columns.values))


        for z in columnas:
            auxiliar = z['name']
            auxiliar = auxiliar.lower()
            auxiliar = auxiliar.replace(" ", "_")
            nombresMetaDatos.append(auxiliar)

            info = columns[0]
            rango = len(info)

            for y in range(rango):
                nombresDatos.append(info.loc[y])

                long_nombresMetaDatos = len(nombresMetaDatos)
                long_nombresDatos = len(nombresDatos)

                if long_nombresDatos > long_nombresMetaDatos:
                    longitud = long_nombresDatos
                else:
                    longitud = long_nombresMetaDatos

                datosComprensibilidad = set(nombresDatos) & set(nombresMetaDatos)
                comprensibilidad = len(datosComprensibilidad)

                indexComprensibilidad = round((1 - (comprensibilidad / longitud)) * 10)
                indexComprensibilidad = int(indexComprensibilidad)

        return indexComprensibilidad

    # -------------------------------------------------------------------------------------------------------------

    def indicadorPortabilidad(self, results):

        if results is not None:
            portabilidad = json.dumps(results)
            indexPortabilidad = 10
        else:
            indexPortabilidad = 0

        indexPortabilidad = int(indexPortabilidad)

        return indexPortabilidad

    # -------------------------------------------------------------------------------------------------------------

    def indicadorConsisetencia(self, dataframe):

        try:

            duplicados = dataframe[dataframe.duplicated(keep=False)]

            shapeDuplicados = duplicados.shape
            numeroDuplicados1 = shapeDuplicados[0]

            numeroRegistros = dataframe.shape
            numeroRegistros1 = numeroRegistros[0]
            indexConsistencia = round(1 - (numeroDuplicados1 / numeroRegistros1)) * 10

        except TypeError as error:

            logging.error('Indicador Consistencia -' + str(error))
            indexConsistencia = 0


        return indexConsistencia

    # -------------------------------------------------------------------------------------------------------------

    def indicadorExactitud(self, results, dataAPI ):

        try:

            nombresDatos, nombresMetaDatos, exactitudVector, calculoExactitud = [], [], [], []
            contadorExactitud, indexExactitud = 0, 0

            columnas = results['columns']
            colz = dataAPI.shape[1]

            # Extrayendo los tipos de datos del conjunto de datos

            for type in columnas:
                exactitudVector.append(type['dataTypeName'])


            if dataAPI.shape[0] < 500:
                iteraciones = dataAPI.shape[0]

            else:
                iteraciones = int(dataAPI.shape[0] / 20)



            for mm in range(colz):

                for jj in range(iteraciones):

                    contadorExactitud = contadorExactitud + 1

                    try:

                        if dataAPI.iloc[jj, mm] is not np.nan:

                            palabra = dataAPI.iloc[jj, mm]
                            palabraAssess = re.findall('\d', palabra)

                            longPalabraResult = len(palabraAssess)
                            longPalabra = len(palabra)

                            if exactitudVector[mm] == 'number' or exactitudVector[mm] == 'floating_timestamp':

                                if longPalabra == longPalabraResult:
                                    calculoExactitud.append(10)

                                else:
                                    calculoExactitud.append(0)

                            elif exactitudVector[mm] == 'text' or exactitudVector[mm] == 'url':

                                if longPalabra == longPalabraResult:
                                    calculoExactitud.append(0)
                                else:
                                    calculoExactitud.append(10)
                        else:

                            calculoExactitud.append(10)



                    except TypeError as error:

                        logging.error('Indicador Consistencia -' +  str(error))
                        calculoExactitud.append(0)

                    for mn in calculoExactitud:
                        indexExactitud = indexExactitud + mn

                    indexExactitud = indexExactitud / contadorExactitud
                    indexExactitud = int(indexExactitud)

        except KeyError as error:

            logging.error('Indicador Exactitud -' + str(error))
            indexExactitud = 0

        return indexExactitud







