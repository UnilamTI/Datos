
# Cabecera para que en ONEBEAT.log se vea la fecha de los warnings
warning("-------------------------------------------------------------------------------------------------------")
warning("---------------------------------------------- ERRORES ----------------------------------------------")

# Librerias
suppressWarnings(library(RODBC))
suppressWarnings(library(taskscheduleR))
suppressWarnings(library(lubridate))
suppressWarnings(library(stringr))
suppressWarnings(library(tidyverse))
suppressWarnings(library(readxl))

# Cabecera para que en ONEBEAT.log se vea la fecha de los warnings
warning(paste0("-------------------  ",today(),"  -------------------"))


# PARENTESIS - CORRO SCRIPT DE CAMBIOS DE PRECIOS ---- 

# Si solo si la hora actual es entre las 2am y las 5am
# Este Script se hizo con el objetivo de registrar cambios de precios en SAP y guardar la fecha de cambio
# Estos cambios se guardan en un csv y un excel en la siguiente direccion:
# "\\192.168.10.100\Datos\Administracion Central\Compras\Análisis de Datos\Cambios de Precio"
if(between( hour(now()) ,2,5)){
  setwd("C:/Scripts/CambiosDePrecios")
  #Script cambios de precios
  source("CambiosDePrecios.R")
  rm(PreciosAyer,PreciosHoy,toSAP,wb)
}

# PARENTESIS - CORRO SCRIPT DE CAMBIOS DE PRECIOS ----


#Objeto que se utiliza para nombrar los archivos final
fechayextension <- paste0("_",str_replace_all(ymd(today()-1),"-",""),"_",hour(now()),minute(now()),floor(second(now())),".csv")

# Cargo restricciones de familias, subfamilias 1 y 2  Y SKUs por locales.
setwd("//192.168.10.100/Datos/Administracion Central/Compras/Análisis de Datos/ScriptsR/ONEBEAT")
restriccionesFamilias <-  read_excel("RESTRICCIONES.xlsx",sheet = 1)
restriccionesSKU <- read_excel("RESTRICCIONES.xlsx",sheet = 2)
#Separo las restricciones de SKU para TODAS y el resto
restriccionesSkuTODAS <- restriccionesSKU[restriccionesSKU[,1]=='TODAS',]
restriccionesSKU <- restriccionesSKU[restriccionesSKU[,1]!='TODAS',]
#Separo las restricciones de SKU para TODAS y el resto
restriccionesSkuMODELO <- restriccionesSKU[restriccionesSKU[,1]=='MODELO',]
restriccionesSKU <- restriccionesSKU[restriccionesSKU[,1]!='MODELO',]

# Cargo los LBRs
LBRs <-  read_excel("LBRs.xlsx",sheet = 1)

# Cargo los grupos de los articulos que se envian juntos
GruposArticulos <-  read_excel("GruposArticulos.xlsx",sheet = 1)

# Cargo los SKUs que se recompran 
RecompraSKUs <-  read_excel("RecompraArticulos.xlsx",sheet = 1)

# Cargo los SKUs excepcionales de textiles que no llevan 1.
TextilesMultiploParentesis <-  as.data.frame(read_excel("MultiployMinimoParentesis.xlsx",sheet = 1))

# Seteo el directorio para poder correr el script que genera los archivos WEB
# setwd("C:/Scripts/ONEBEAT")
# source("Sources/SustitutosWEB-ONEBEAT.R")

# Directorio 
setwd("//UNILAMONEBEAT/InputFolder")

# Conector ODBC a CZZ

toCZZ <- odbcConnect("toCZZ",uid = "ConsultaCZZ",pwd = "Consulta.g0624")

# Cargar datos de comerzzia
StockEnTransitoCZZ <- sqlQuery(toCZZ,"  SELECT T1.[CODALM_DESTINO] AS 'Deposito Destino'
        ,T2.[COD_ARTICULO_SAP] AS 'SKU Sap'
        ,CAST((SUM (T2.[CANTIDAD])) AS INT) AS 'EnTransitoCMZ'
  FROM [comerzzia-pro].[comerzzia].[Z_PBI_MERCANCIAS_DET_PENDIENTES_RECEPCIONAR] T2
  INNER JOIN (SELECT [ID_CLIE_ALBARAN],[CODALM_DESTINO] FROM [comerzzia-pro].[comerzzia].[Z_PBI_MERCANCIAS_PENDIENTES_RECEPCIONAR]
             WHERE UID_ACTIVIDAD = 'A' AND [CODALM_DESTINO] NOT IN ('0001','')
			 GROUP BY [ID_CLIE_ALBARAN],[CODALM_DESTINO]) T1 ON T1.[ID_CLIE_ALBARAN] = T2.[ID_CLIE_ALBARAN]
			 WHERE COD_ARTICULO_SAP IS NOT NULL
  GROUP BY T1.[CODALM_DESTINO], T2.[COD_ARTICULO_SAP] UNION ALL
  (SELECT 'A' AS 'Deposito Destino', 'B' AS 'SKU Sap', 1 AS 'EnTransitoCMZ')")
odbcClose(toCZZ)

# Conector ODBC a SAP
toSAP <- odbcConnect("hanab1",uid = "DATAWUSER",pwd = "Unilam2023")
# Cargar datos de SAP
LOCATION <- sqlQuery(toSAP,'SELECT "Stock Location Name", "Stock Location Description", "Stock Location Type", "Ciudad", "Zona", "Tamaño", M2, "Categorizacion", "Marca", "Tipo de Local", "Reported Date Year", "Reported Date Month", "Reported Date Day"
FROM PRD_UNILAM.ONEBEAT_LOCATIONS')
TRANSACTION <- sqlQuery(toSAP,'SELECT "TransactionID", "Origen", SKU, "Destination", "Type", "Adjust_inventory", "Quantity", "Selling Price", "Reported Date Year", "Reported Date Month", "Reported Date Day"
FROM PRD_UNILAM.ONEBEAT_TRANSACTIONS')
MTSKUS <- sqlQuery(toSAP,'SELECT "Stock Location", SKU, "SKU Description", "Buffer Size(inicial)", "Origin stock location", "Replenishment Time", "Avoid Replenishment", "Inventory At Production", "Inventory At Transit", "Inventory At Site", "Unit Price", "TVC (Unit Cost)", "Throughput", "Minimum Replenishment", "Replenishment Multiplications", "Last Batch Replenishment", "Shiping Measure", "DBM Policy (Buffer Management Policy)", "Modelo", COLOR, TALLE, "Familia", "Subfamilia", "Subfamilia2", "Atributo", "Marca", "Numero de compra", "Reclasificacion a Outlet", "Destino", "Propiedad", "Full Date", "Reported Date Year", "Reported Date Month", "Reported Date Day"
FROM PRD_UNILAM.ONEBEAT_MTSKUS')
STATUS <- sqlQuery(toSAP,'SELECT "Stock Location", SKU, "Inventory At Hand", "Inventory On The Way", "Reported Date Year", "Reported Date Month", "Reported Date Day"
FROM PRD_UNILAM.ONEBEAT_STATUS')
invcontenedor <- sqlQuery(toSAP,'SELECT "Código Artículo", "Descripción", "Ubicación", "Código de Zona", "Nombre Zona", "Estado de Calidad", "Stock Disponible Repo" AS "Cantidad"
FROM PRD_UNILAM."FRAN_InventarioPorContenedorYEntrepiso"')
DevsEnTiendaIncorrecta <- sqlQuery(toSAP,'SELECT DISTINCT "ItemCodeDev", "SucursalDev" FROM PRD_UNILAM.ONEBEAT_DEVOLUCIONES_SUCEQUIVOCADA;')


odbcClose(toSAP)

# LOCATION----
#Remplazo las columnas NA de 4 ciudad, 5 zona, 6 tamanio, 7 M2, 8 categoria, 9 Marca y 10 tipo local por N/A
LOCATION[is.na(LOCATION[,4]),4] <- 'N/A'
LOCATION[is.na(LOCATION[,5]),5] <- 'N/A'
LOCATION[is.na(LOCATION[,6]),6] <- 'N/A'
LOCATION[is.na(LOCATION[,7]),7] <- 'N/A'
LOCATION[is.na(LOCATION[,8]),8] <- 'N/A'
LOCATION[is.na(LOCATION[,9]),9] <- 'N/A'
LOCATION[is.na(LOCATION[,10]),10] <- 'N/A'

#----


# TRANSACTION ----

# Elimino la ultima fila que era solo de auxiliar para que queden algunas columnas caracter
TRANSACTION <- TRANSACTION[-nrow(TRANSACTION),]

# Elimino articulos que no tienen sentido como ENVIOWEB, GENBASICO, PEYAEXPRESS, PEYACOORDINADO, COMPDEV
TRANSACTION <- TRANSACTION[TRANSACTION$SKU!='ENVIOWEB',]
TRANSACTION <- TRANSACTION[TRANSACTION$SKU!='GENBASICO',]
TRANSACTION <- TRANSACTION[TRANSACTION$SKU!='PEYAEXPRESS',]
TRANSACTION <- TRANSACTION[TRANSACTION$SKU!='PEYACOORDINADO',]
TRANSACTION <- TRANSACTION[TRANSACTION$SKU!='COMPDEV',]

#Elimino filas con NA EN DESTINATION
TRANSACTION <- TRANSACTION[!(is.na(TRANSACTION$Destination)),]



# Cambio los valores 99999 en selling price, significa que van vacios
TRANSACTION[TRANSACTION[,8]=='99999',8] <- ''

# TEMPORAL, quita los datos NA en Origen (Estos datos se generan cuando no se referencian documentos en SAP)
TRANSACTION <- TRANSACTION[!is.na(TRANSACTION[,2]),]

# TRANSACTIONS que no existen en LOCATIONS se remplazan por EXTERNAL
TRANSACTION[!(TRANSACTION$Origen %in% LOCATION$`Stock Location Name`),2] <- 'EXTERNAL'
TRANSACTION[!(TRANSACTION$Destination %in% LOCATION$`Stock Location Name`) & TRANSACTION$Destination!= 'CLIENT',4] <- 'EXTERNAL' 

# Al remplazar algunas LOCATIONS por EXTERNAL quedan transactions que no son coherentes como INs a EXTERNAL cuando ya se contabilizo esta salida
# o OUTs desde EXTERNAL cuando ya se contabilizo esta entrada a la tienda. Por tanto se filtran
TRANSACTION <- TRANSACTION[!(TRANSACTION$Destination=='EXTERNAL' & TRANSACTION$Type=='IN'),]
TRANSACTION <- TRANSACTION[!(TRANSACTION$Origen=='EXTERNAL' & TRANSACTION$Type=='OUT'),]

# Quito todas las filas donde el agregar EXTERNAL hace que no tengan sentido
TRANSACTION <- TRANSACTION[!(TRANSACTION$Origen=='EXTERNAL' & TRANSACTION$Destination=='CLIENT'),]
TRANSACTION <- TRANSACTION[!(TRANSACTION$Origen=='EXTERNAL' & TRANSACTION$Destination=='EXTERNAL'),]

# Agrego un ID de fila al ID-Transaction para asegurarnos que no se repitan
TRANSACTION$TransactionID <- paste0(TRANSACTION$TransactionID,"_",1:nrow(TRANSACTION))

# Filtro los FALTACASO
TRANSACTION <-  TRANSACTION[TRANSACTION$Origen!='FALTACASO',]

#----

# MTSKUS ----

# Elimino la ultima fila que era solo de auxiliar para que queden algunas columnas caracter
MTSKUS <- MTSKUS[-nrow(MTSKUS),]

# En las columnas 14 Minimum Replenishment  y 15 Replenishment Multiplications, Si estan vacias remplazo por el pak de la familia correspondiente
MTSKUS[is.na(as.numeric(MTSKUS[,14]))&str_sub(MTSKUS[,22],str_length(MTSKUS[,22])-1,str_length(MTSKUS[,22])-1) %in% c("V","M","N","T","K","I","L","A","E","J","W","X","B","C"),14] <- 12
MTSKUS[is.na(as.numeric(MTSKUS[,14]))&str_sub(MTSKUS[,22],str_length(MTSKUS[,22])-1,str_length(MTSKUS[,22])-1) %in% c("H","F","D","O","P","R"),14] <- 10
MTSKUS[is.na(as.numeric(MTSKUS[,15]))&str_sub(MTSKUS[,22],str_length(MTSKUS[,22])-1,str_length(MTSKUS[,22])-1) %in% c("V","M","N","T","K","I","L","A","E","J","W","X","B","C"),15] <- 12
MTSKUS[is.na(as.numeric(MTSKUS[,15]))&str_sub(MTSKUS[,22],str_length(MTSKUS[,22])-1,str_length(MTSKUS[,22])-1) %in% c("H","F","D","O","P","C","R"),15] <- 10

# Combierto las columnas 4 Buffer Size(inicial), 14 Minimum Replenishment  y 15 Replenishment Multiplications en numerico para sacar valores raros
# y despues remplazo los NA por vacios
MTSKUS[,4] <- as.numeric(MTSKUS[,4])
MTSKUS[ is.na( MTSKUS[,4] ),4 ] <- ""
MTSKUS[,14] <- as.numeric( MTSKUS[,14] )
MTSKUS[is.na(MTSKUS[,14]),14] <- ""
MTSKUS[,15] <- as.numeric(MTSKUS[,15])
MTSKUS[is.na(MTSKUS[,15]),15] <- ""

# Buffer inicial a los articulos que no tienen les pongo Minimum Replenishment
MTSKUS[MTSKUS$`Buffer Size(inicial)`=='',4] <- MTSKUS[MTSKUS$`Buffer Size(inicial)`=='',14]

# Buffer inicial a los articulos que siguen sin tener les pongo 1
MTSKUS[MTSKUS$`Buffer Size(inicial)`=='',4] <- 1

# Sub familias y sub familias 2 NA
MTSKUS[is.na(MTSKUS$Subfamilia),23] <- 'N/A'
MTSKUS[is.na(MTSKUS$Subfamilia2),24] <- 'N/A'

#Sin precio les pongo 0
MTSKUS[is.na(MTSKUS[,11]),11] <- 0

#Sin costo les pongo 0
MTSKUS[is.na(MTSKUS[,12]),12] <- 0

#Sin dif precio costo les pongo 0 y negativos
MTSKUS[is.na(MTSKUS[,13]),13] <- 0
MTSKUS[MTSKUS[,13]<0,13] <- 0

#Sin color les pongo 00
MTSKUS[is.na(MTSKUS[,20]),20] <- '00'

#Sin talle les pongo 00
MTSKUS[is.na(MTSKUS[,21]),21] <- '00'

#Sin marca les pongo N/A
MTSKUS[is.na(MTSKUS[,26]),26] <- 'N/A'

#Sin num compra les pongo N/A
MTSKUS[is.na(MTSKUS[,27]),27] <- 'N/A'

#Sin destinoles pongo N/A
MTSKUS[is.na(MTSKUS[,29]),29] <- 'N/A'

# Remplazo lOS PUNTO Y COMA de la descripcion por guiones
MTSKUS[,3] <- str_replace_all(MTSKUS[,3],";","-")

# Remplazo las comas de la descripcion por guines bajos
MTSKUS[,3] <- str_replace_all(MTSKUS[,3],",","_")

# Remplazo las comillas de la familia por vacio
MTSKUS[,22] <- str_replace_all(MTSKUS[,22],'"',"")

# Inventario por contenedor
#Preparo "invcontenedor" 
invcontenedor <- cbind(1:nrow(invcontenedor),invcontenedor)
invcontenedor <- invcontenedor[,c(2,3,4,5,7,8)]
invcontenedor <- as.data.frame(invcontenedor)
names(invcontenedor) <- c("SKU","desc","ubicacion","codigozona","estadodecalidad","cantidad")
invcontenedor <- invcontenedor[invcontenedor$estadodecalidad=="LIBERADO CONTENEDOR",]
invcontenedorstatus <- invcontenedor # Guardo una copia de invcontenedor que mantiene las cantidades disponibles
invcontenedor <- invcontenedor$SKU[!duplicated(invcontenedor$SKU)]

# Los SKUs que existen en LIBERADO CONTENEDOR se cambia la columna 7 Avoid Replenishment por 0, significa que se les hace reposicion.
MTSKUS[(MTSKUS$SKU %in% invcontenedor),7] <- 0

# Restricciones de familias
# Creo filtros con las restricciones
# No vende
aux <- restriccionesFamilias[is.na(restriccionesFamilias$SUB) & restriccionesFamilias$COMENTARIO=='NO VENDE',]
FiltroFam <- paste0(aux$SUCURSAL,aux$FAM)
aux <- (restriccionesFamilias[!(is.na(restriccionesFamilias$SUB)) & (is.na(restriccionesFamilias$SUBDOS)) & restriccionesFamilias$COMENTARIO=='NO VENDE',])
FiltroSFam <- paste0(aux$SUCURSAL,aux$FAM,aux$SUB)
rm(aux)
aux <- (restriccionesFamilias[!(is.na(restriccionesFamilias$SUBDOS)) & restriccionesFamilias$COMENTARIO=='NO VENDE',])
FiltroS2Fam <- paste0(aux$SUCURSAL,aux$FAM,aux$SUB,aux$SUBDOS)
rm(aux)
# Solo vende
aux <- (restriccionesFamilias[!(is.na(restriccionesFamilias$SUB)) & (is.na(restriccionesFamilias$SUBDOS)) & restriccionesFamilias$COMENTARIO=='SOLO VENDE',])
SoloVendeFiltroSFam <- paste0(aux$SUCURSAL,aux$FAM,aux$SUB)
rm(aux)
aux <- (restriccionesFamilias[!(is.na(restriccionesFamilias$SUBDOS)) & restriccionesFamilias$COMENTARIO=='SOLO VENDE',])
SoloVendeFiltroS2Fam <- paste0(aux$SUCURSAL,aux$FAM,aux$SUB,aux$SUBDOS)
rm(aux)

# Aplico filtro de familias NO VENDE
MTSKUS[ # Filas del MTSKUS que se les reparte mercaderia
         (MTSKUS$`Avoid Replenishment`==0) &
        # Filas del MTSKUS sucursales a restringir solo familia
         (paste0(MTSKUS$`Stock Location`,str_sub(MTSKUS$Familia, -1)) %in% FiltroFam),7] <- 1

# Aplico filtro de subfamilias NO VENDE
MTSKUS[ # Filas del MTSKUS que se les reparte mercaderia
  (MTSKUS$`Avoid Replenishment`==0) &
    # Filas del MTSKUS sucursales a restringir solo familia
    (paste0(MTSKUS$`Stock Location`,str_sub(MTSKUS$Familia, -1),str_sub(MTSKUS$Subfamilia , 1,1)) %in% FiltroSFam),7] <- 1

# Aplico filtro de subfamilias2 NO VENDE
MTSKUS[# Filas del MTSKUS que se les reparte mercaderia
  (MTSKUS$`Avoid Replenishment`==0) &
    # Filas del MTSKUS sucursales a restringir solo familia
    (paste0(MTSKUS$`Stock Location`,str_sub(MTSKUS$Familia, -1),str_sub(MTSKUS$Subfamilia , 1,1),str_sub(MTSKUS$Subfamilia2 , 1,1)) %in% FiltroS2Fam),7] <- 1

# Aplico filtro de subfamilias SOLO VENDE
MTSKUS[ # Filas del MTSKUS que se les reparte mercaderia
  (MTSKUS$`Avoid Replenishment`==0) &
    # Filas del MTSKUS sucursales a restringir solo familia
    !(paste0(MTSKUS$`Stock Location`,str_sub(MTSKUS$Familia, -1),str_sub(MTSKUS$Subfamilia , 1,1)) %in% SoloVendeFiltroSFam) & 
     (paste0(MTSKUS$`Stock Location`,str_sub(MTSKUS$Familia, -1)) %in% (substr(SoloVendeFiltroSFam, 1, nchar(SoloVendeFiltroSFam)-1))) ,7] <- 1

# Aplico filtro de subfamilias2 SOLO VENDE
MTSKUS[ # Filas del MTSKUS que se les reparte mercaderia
  (MTSKUS$`Avoid Replenishment`==0) &
    # Filas del MTSKUS sucursales a restringir solo familia
    !(paste0(MTSKUS$`Stock Location`,str_sub(MTSKUS$Familia, -1),str_sub(MTSKUS$Subfamilia , 1,1),str_sub(MTSKUS$Subfamilia2 , 1,1)) %in% SoloVendeFiltroS2Fam) & 
    (paste0(MTSKUS$`Stock Location`,str_sub(MTSKUS$Familia, -1)) %in%  (substr(SoloVendeFiltroS2Fam, 1, nchar(SoloVendeFiltroS2Fam)-2))) ,7] <- 1


# Restricciones de articulos Una Sucursal
MTSKUS[paste0(MTSKUS$`Stock Location`,MTSKUS$SKU) %in% paste0(restriccionesSKU$sucursal,restriccionesSKU$SKU),7] <- 1
# Restricciones de articulos TODAS Sucursales
MTSKUS[MTSKUS$SKU %in% restriccionesSkuTODAS$SKU,7] <- 1
# Restricciones de articulos por MODELOS
MTSKUS[MTSKUS$Modelo %in% restriccionesSkuMODELO$SKU,7] <- 1

if(nrow(LBRs)>0){
  # Agrego los LBRs del excel LBRs.xlsx
  #quito posibles duplicados
  LBRs %>% group_by(sucursal,SKU) %>% summarise(LBR = first(LBR))
  #renombro LBRs
  names(LBRs) <- c("Stock Location","SKU", "LBR")
  #combierto a dataframe
  LBRs <- as.data.frame(LBRs)
  #joineo MTSSKUS con LBRs
  MTSKUS <- left_join(MTSKUS, LBRs, by = c("SKU", "Stock Location"))
  #agrego los LBRs joineados y mantengo los otros
  MTSKUS[is.na(MTSKUS$LBR),35] <- MTSKUS[is.na(MTSKUS$LBR),16]
  #remplazo Last Batch Rempleishment por la nueva columna actualizada LBR
  MTSKUS$`Last Batch Replenishment` <- MTSKUS$LBR
  #Borro la ultima columna del MTSKUS, LBR, que es auxiliar
  MTSKUS <- MTSKUS[,1:34]
  
}


# Quito combinaciones de SKU y Sucursal que se devolvieron en la tienda equivocada.
# Elimino la ultima fila que era solo de auxiliar para que quede la columna de sucursal como caracter
DevsEnTiendaIncorrecta <- DevsEnTiendaIncorrecta[-nrow(DevsEnTiendaIncorrecta),]
names(DevsEnTiendaIncorrecta) <- c("SKU","Stock Location")

# Agrego una columna para que al hacer join quede algun registro de que cruza correctamente y que no 
DevsEnTiendaIncorrecta$columna <- "valor"

# Cruzo el MTSSKUS con las devoluviones y cambio avoid remplashment(columna 7) por 1, para las devoluciones en tienda incorrecta.
MTSKUS <- left_join(MTSKUS, DevsEnTiendaIncorrecta, by = c("SKU", "Stock Location"))
MTSKUS[!(is.na(MTSKUS$columna)),7] <- 1

#Elimino la ultima columna del MTSSKUS que es auxuliar para las devoluciones
MTSKUS <- MTSKUS[,1:(ncol(MTSKUS)-1)]

# Elimino las comillas y los saltos de linea de las descripciones "\r"
MTSKUS$`SKU Description` <- str_remove_all(MTSKUS$`SKU Description`,'"')
MTSKUS$`SKU Description` <- str_remove_all(MTSKUS$`SKU Description`,'\r')


# No se reportan los SKUs Sustitutos o SKUs web en el MTSSKUS 
# Por lo tanto se filtran
# SKUsSustitutosWeb <- AperturasFinal[!(AperturasFinal$SKUweb %in% AperturasFinal$SKUdepo),2]
# SKUsSustitutosWeb <- SKUsSustitutosWeb[!(duplicated(SKUsSustitutosWeb))]

# Filtro los sustitutos de la sucursal 30 en el MTSSKUS
# MTSKUS[(MTSKUS$SKU %in% SKUsSustitutosWeb) ,1] <- "QUITAR"
# MTSKUS <- MTSKUS[MTSKUS$`Stock Location`!="QUITAR",]

# AGREGAR NUEVA COLUMNA QUE AGRUPO LOS CODIGOS PARA ENVIAR
# Agregar grupos de articulos
# Renombro las columnas de grupos articulos 
if( nrow(GruposArticulos) > 0 ){
  names(GruposArticulos) <- c("SKU","GrupoSKU")
  # Agrego los grupos de SKU
  MTSKUS <- left_join(MTSKUS,GruposArticulos,by='SKU')
  # Remplazo NAs por vacio
  MTSKUS[ is.na(MTSKUS$GrupoSKU) , 35] <- 0
  # Reordeno las columnas en el orden que debe ser
  MTSKUS <- MTSKUS[,c(1:31,35,32,33,34)]
}


# AGREGAR NUEVA COLUMNA QUE IDENTIFICA CODIGOS DISTINTOS QUE SON MISMO ARTICULO.
# Renombro las columnas
#if( nrow(RecompraSKUs) > 0 ){
#  names(RecompraSKUs) <- c("SKU","RecompraSKU")
#  # Agrego los grupos de SKU
#  MTSKUS <- left_join(MTSKUS,RecompraSKUs,by='SKU')
#  # Remplazo NAs por vacio
#  MTSKUS[ is.na(MTSKUS$RecompraSKU) , 36] <- 0
#  # Reordeno las columnas en el orden que debe ser
#  MTSKUS <- MTSKUS[,c(1:32,36,33,34,35)]
#}



# Las familias de Textiles llevan multiplo y minimo de repo 1
# Estas son HOMBRES M" ; "NIÑOS N" ; "TEENS T" ; "BEBES K" ; "INTERIOR I" ; "DAMA V"
#if (nrow(TextilesMultiploParentesis)!=0){
  
  MTSKUS[ (MTSKUS$Familia %in% c("DAMA V","MINI KIDS N","HOMBRES M","KIDS T","BABY K","INTERIOR I")) &
            !(MTSKUS$`Stock Location` %in% c("0002","0004","0014")) &
            !(MTSKUS$SKU %in% (TextilesMultiploParentesis$SKU)),14] <- 1 
  
  MTSKUS[ (MTSKUS$Familia %in% c("DAMA V","MINI KIDS N","HOMBRES M","KIDS T","BABY K","INTERIOR I")) &
            !(MTSKUS$`Stock Location` %in% c("0002","0004","0014")) &
            !(MTSKUS$SKU %in% (TextilesMultiploParentesis$SKU)),15] <- 1 

# ----




# STATUS----


# Elimino la ultima fila que era solo de auxiliar para que queden algunas columnas caracter
STATUS <- STATUS[-nrow(STATUS),]

# Selecciono solo las columnas SKU y cantidad (cantidad disponible en depo) y quito duplicados a invcontenedorstatus
invcontenedorstatus <- invcontenedorstatus[,c(1,6)]
invcontenedorstatus <- invcontenedorstatus[!(duplicated(invcontenedorstatus$SKU)),]

# El stock del 0001 del STATUS se remplaza por lo que hay en liberado contenedor.
nuevostockdepo <- left_join(STATUS,invcontenedorstatus,by='SKU')[STATUS$`Stock Location`=='0001',8] # creo vector de stock en depo de invcontenedor
nuevostockdepo[is.na(nuevostockdepo)] <- 0 # remplazo NAs por cero
STATUS[STATUS$`Stock Location`=='0001',3] <- nuevostockdepo # Remplazo el stock de depo por el de invcontenedor

#Borro estos objetos que ya use
rm(invcontenedorstatus)
rm(nuevostockdepo)

# Agrego el stock transitorio de CZZ a la columna Inventory On The Way (columna 4)
# Preparo el stock transitorio de CZZ
StockEnTransitoCZZ <- StockEnTransitoCZZ[-nrow(StockEnTransitoCZZ),] # La ultima fila se agregar para traer las suc con ceros, por lo tanto la borro
names(StockEnTransitoCZZ) <- c("Stock Location","SKU","Inventory On The Way")

#la columna stock location viene con espacios por lo cual se eliminan
StockEnTransitoCZZ$`Stock Location` <- str_remove_all(StockEnTransitoCZZ$`Stock Location`," ")

# Nueva columna de stock en transito
nuevoStockTransitorioCZZ <- left_join(STATUS,StockEnTransitoCZZ,by=c('Stock Location','SKU'))[,8]
nuevoStockTransitorioCZZ[is.na(nuevoStockTransitorioCZZ)] <- 0 # Remplazo NAs por cero

# Usando la nueva columna de stock en transito comerzzia
# Le sumo al stock en transito de sol traslado de SAP el transito de comerzzia
STATUS[,4] <- STATUS[,4] + nuevoStockTransitorioCZZ 
# Articulos que el stock negativo en sucursal es menor en valor absoluto, que el transito, le resto al transito el stock negativo
STATUS[STATUS$`Inventory At Hand`<0 & STATUS$`Inventory On The Way`>0 & ((STATUS$`Inventory At Hand`+ STATUS$`Inventory On The Way`)>0),4] <-
  STATUS[STATUS$`Inventory At Hand`<0 & STATUS$`Inventory On The Way`>0 & ((STATUS$`Inventory At Hand`+ STATUS$`Inventory On The Way`)>0),3] + 
  STATUS[STATUS$`Inventory At Hand`<0 & STATUS$`Inventory On The Way`>0 & ((STATUS$`Inventory At Hand`+ STATUS$`Inventory On The Way`)>0),4]
# Articulos que tienen negativo en depo
STATUS[STATUS$`Inventory At Hand`<0,3] <- 0


#Borro estos objetos que ya use
#rm(StockEnTransitoCZZ)
#rm(nuevoStockTransitorioCZZ)

# ----


write.table(TRANSACTION,paste0("TRANSACTIONS",fechayextension),row.names = FALSE, quote = FALSE,sep = ";")
write.table(MTSKUS,paste0("MTSSKUS",fechayextension),row.names = FALSE, quote = FALSE,sep = ";")
write.table(LOCATION,paste0("STOCKLOCATIONS",fechayextension),row.names = FALSE, quote = FALSE,sep = ";")
write.table(STATUS,paste0("STATUS",fechayextension),row.names = FALSE, quote = FALSE,sep = ";")
#write.table(AperturasFinal,paste0("SUBSTITUTE_SKUS",fechayextension),row.names = FALSE, quote = FALSE,sep = ";")

# -------Borrar sucursal-------

# Seteo el directorio
# setwd("C:/Scripts/ONEBEAT")

# cargo datos de la sucursal
# borrarsucursal <- read.csv2("borrarsuc42.csv")
# creo el archivo para borrar sucursal de MTSSKUS
# DELETEMTSKUS <- data.frame(sucursal=borrarsucursal$Stock.Location ,
#           skus=borrarsucursal$SKU ,
#           anio=rep('2023',length(borrarsucursal$SKU)),
#           mes=rep('12',length(borrarsucursal$SKU)),
#           dia=rep('05',length(borrarsucursal$SKU)) )
# Le agrego los ceros a las sucursales si es necesario
# DELETEMTSKUS[str_length(DELETEMTSKUS$sucursal)==1,1] <- paste0("000",DELETEMTSKUS[str_length(DELETEMTSKUS$sucursal)==1,1])
# DELETEMTSKUS[str_length(DELETEMTSKUS$sucursal)==2,1] <- paste0("00",DELETEMTSKUS[str_length(DELETEMTSKUS$sucursal)==2,1])
# DELETEMTSKUS[str_length(DELETEMTSKUS$sucursal)==3,1] <- paste0("0",DELETEMTSKUS[str_length(DELETEMTSKUS$sucursal)==3,1])

# renombro las columnas
# names(DELETEMTSKUS) <- c("Stock Location Name","SKU Name","Reported Year","Reported Month","Reported Day")
# exporto el archivo en input folders
# setwd("//UNILAMONEBEAT/InputFolder")
# write.table(DELETEMTSKUS,paste0("DELETEMTSSKUS",fechayextension),row.names = FALSE, quote = FALSE,sep = ";")

#creo el archivo para borrar la sucursal del location
# DELETELOCATION <- data.frame(nombresucursal=c('0042'),
#                             anio=rep('2023',1), mes=rep('12',1), dia=rep('05',1))
# names(DELETELOCATION) <- c("Stock Location Name","Reported Year","Reported Month","Reported Day")
# exporto el archivo en input folders
# write.table(DELETELOCATION,paste0("DELETESTOCKLOCATIONS",fechayextension),row.names = FALSE, quote = FALSE,sep = ";")

#-------Borrar sucursal-------


# ---- Guardo el STATUS y MTSSKUS para registrar cambios ----
write.table(MTSKUS,"C:/Scripts/ONEBEAT/STATUS Y MTSSKUS DE AYER/MTSSKUS_ayer.csv",row.names = FALSE, quote = FALSE,sep = ";")
write.table(STATUS,"C:/Scripts/ONEBEAT/STATUS Y MTSSKUS DE AYER/STATUS_ayer.csv",row.names = FALSE, quote = FALSE,sep = ";")
# ---- Guardo el STATUS y MTSSKUS para registrar cambios ----

# Limpio el LOG de errores de ayer----
file.remove("C:/Scripts/ONEBEAT/ONEBEAT.log")
# Limpio el LOG de errores de ayer----

# Pie para que en ONEBEAT.log se vea la fecha de los warnings
warning(paste0("--------------------------------------  ",today(),"  --------------------------------------"))
warning("-------------------------------------------------------------------------------------------------------")
