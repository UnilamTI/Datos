
# Cabecera para que en ONEBEAT.log se vea la fecha de los warnings
warning("-------------------------------------------------------------------------------------------------------")
warning("---------------------------------------------- ERRORES ----------------------------------------------")

# Librerias
suppressWarnings(library(RODBC))
suppressWarnings(library(taskscheduleR))
suppressWarnings(library(lubridate))
suppressWarnings(library(stringr))
suppressWarnings(library(tidyverse))

# Cabecera para que el .log se vea la fecha de los warnings
warning(paste0("-------------------  ",today(),"  -------------------"))

#Objeto que se utiliza para nombrar los archivos final
fechayextension <- paste0("_",str_replace_all(ymd(today()-1),"-",""),"_",hour(now()),minute(now()),floor(second(now())),".csv")

# setwd("C:/Scripts/ONEBEAT/Registro_Facturas_Pasadas")
setwd("C:/Scripts/ONEBEAT")

# Cargo el transactions
IdFacturas <- read.csv("Transactions History.csv",sep = ";")
# Quito las lineas que tienen fecha vacia
IdFacturas <- IdFacturas[!IdFacturas$Reported.Date=='',]
# Quito las lineas que tienen fecha NA
IdFacturas <- IdFacturas[!is.na(IdFacturas$Reported.Date) , ]


# Guardo la minima y maxima fecha del Transaction History
MinFechaTransacH <- min(as.Date( IdFacturas$Reported.Date ,format = "%d/%m/%Y %H:%M:%S") )
MaxFechaTransacH <- max(as.Date( IdFacturas$Reported.Date ,format = "%d/%m/%Y %H:%M:%S") )
# Vuelvo a cambiar el formato de las fechas, asi que da de la forma AñoMesDia para consultar en SAP
MinFechaTransacH <- paste0(year(MinFechaTransacH),sprintf("%02d",month(MinFechaTransacH)),sprintf("%02d",day(MinFechaTransacH)) )
MaxFechaTransacH <- paste0(year(MaxFechaTransacH),sprintf("%02d",month(MaxFechaTransacH)),sprintf("%02d",day(MaxFechaTransacH)) )

#Me quedo con la primera columna 
IdFacturas <- IdFacturas[,1]
# Combierto a data frame
IdFacturas <- as.data.frame(IdFacturas)
# renombro la columna
names(IdFacturas) <- "Transaction"

# devoluciones
devoluciones <- IdFacturas[str_detect(IdFacturas$Transaction,"DEVO"),]
devoluciones <- str_sub(devoluciones,6,15)
devoluciones <- str_sub(devoluciones,1,str_locate(devoluciones,"_")[,1]-1)
devoluciones <- devoluciones[!duplicated(devoluciones)]

# ventas
ventas <- IdFacturas[str_detect(IdFacturas$Transaction,"VENT"),]
ventas <- str_sub(ventas,6,15)
ventas <- str_sub(ventas,1,str_locate(ventas,"_")[,1]-1)
ventas <- ventas[!duplicated(ventas)]

# Todos los documentos de ventas y devoluciones
ventas <- as.data.frame(ventas)
devoluciones <- as.data.frame(devoluciones)
names(devoluciones) <- "ventas"
facturasONEBEAT <- as.data.frame(rbind(ventas,devoluciones))
facturasONEBEAT <- facturasONEBEAT[!duplicated(facturasONEBEAT$ventas),]

# Combierto a data frame y cambio el nombre de la columna
facturasONEBEAT <- as.data.frame(facturasONEBEAT)
names(facturasONEBEAT) <- "ventas"

############ Facturas que hemos pasado a ONEBEAT ############ 
#############################################################



####################################################
############ Facturas que estan en SAP ############ 

# Conector ODBC a SAP
toSAP <- odbcConnect("hanab1",uid = "DATAWUSER",pwd = "Unilam2023")

# Creo la vista generica para consultar las facturas pero sin las fechas
Vista  <- "SELECT \"NroComprobante\", \"FechaCreación\", \"HoraCreción\", \"FechaComprobante\" 
                  FROM PRD_UNILAM.\"FRAN_FacturasSAP\"
                  WHERE \"FechaComprobante\" >= 'FechaMinTransaction'
                  AND \"FechaComprobante\" <= 'FechaMaxTransaction' "
# Le agrego las fechas Minima y maxima del transaction a la vista
Vista <- str_replace(Vista,"FechaMinTransaction",MinFechaTransacH)
Vista <- str_replace(Vista,"FechaMaxTransaction",MaxFechaTransacH)


# Cargar datos de SAP
FacturasSAP <- sqlQuery(toSAP, Vista)
FacturasSAP <- FacturasSAP[!duplicated(FacturasSAP[,1]),]
odbcClose(toSAP)
rm(toSAP)
rm(Vista)

FacturasSAP <- cbind(FacturasSAP,FacturasSAP$NroComprobante %in% facturasONEBEAT$ventas )
FacturasSAP <- as.data.frame(FacturasSAP)
names(FacturasSAP)[5] <- "estaEnOnebeat"
FacturasSAP$fechasdistintas <- !(FacturasSAP$FechaCreación==FacturasSAP$FechaComprobante)

# limpieza
rm(devoluciones)
rm(ventas)
rm(IdFacturas)
rm(facturasONEBEAT)

############ Facturas que estan en SAP ############ 
####################################################


# Todas las transferencias VENT y DEVO SAP
toSAP <- odbcConnect("hanab1",uid = "DATAWUSER",pwd = "Unilam2023")

# Creo la vista generica para todas las ventas y devoluciones con el formato ONEBEAT
Vista <- "SELECT \"TransactionID\", \"Origen\", SKU, \"Destination\", \"Type\", \"Adjust_inventory\", \"Quantity\", \"Selling Price\", \"Reported Date Year\", \"Reported Date Month\", \"Reported Date Day\"
          FROM PRD_UNILAM.FRAN_TODAS_TRANSFERENCIAS_VENTYDEVO
            WHERE \"FechaCompleta\" >= 'FechaMinTransaction' AND \"FechaCompleta\" <= 'FechaMaxTransaction' "
# Le agrego las fechas Minima y maxima del transaction a la vista
Vista <- str_replace(Vista,"FechaMinTransaction",MinFechaTransacH)
Vista <- str_replace(Vista,"FechaMaxTransaction",MaxFechaTransacH)

# Cargar datos de SAP
todastransfSAP <- sqlQuery(toSAP,Vista)
LOCATION <- sqlQuery(toSAP,'SELECT "Stock Location Name", "Stock Location Description", "Stock Location Type", "Ciudad", "Zona", "Tamaño", M2, "Categorizacion", "Marca", "Tipo de Local", "Reported Date Year", "Reported Date Month", "Reported Date Day"
FROM PRD_UNILAM.ONEBEAT_LOCATIONS')
odbcClose(toSAP)
rm(toSAP)
rm(Vista)
# quito nulos
todastransfSAP <- todastransfSAP[!is.null(todastransfSAP$TransactionID),]

# Quito la ultima columna de fecha ya que es auxiliar para la consulta
#todastransfSAP <- todastransfSAP[,-ncol(todastransfSAP)]

# Le agrego los ceros a las sucursales que les falta
todastransfSAP$Origen <- sprintf("%04d", todastransfSAP$Origen )

# Facturas faltantes
FacturasFaltantes <- FacturasSAP[FacturasSAP$estaEnOnebeat==FALSE,]
FacturasFaltantes <- FacturasFaltantes[!is.na(FacturasFaltantes$NroComprobante),]

# Extraido documentos de todas las transferencias de ventas y devolusiones
aux <- str_sub(todastransfSAP$TransactionID,6,15)
aux <- str_sub(aux,1,str_locate(aux,"_")[,1]-1)

# Lineas del transaction que faltan en ONEBEAT
TRANSACTION <- todastransfSAP[aux %in% FacturasFaltantes$NroComprobante,]


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

# Elimino articulos que no tienen sentido como ENVIOWEB, GENBASICO, PEYAEXPRESS, PEYACOORDINADO, COMPDEV
TRANSACTION <- TRANSACTION[TRANSACTION$SKU!='ENVIOWEB',]
TRANSACTION <- TRANSACTION[TRANSACTION$SKU!='GENBASICO',]
TRANSACTION <- TRANSACTION[TRANSACTION$SKU!='PEYAEXPRESS',]
TRANSACTION <- TRANSACTION[TRANSACTION$SKU!='PEYACOORDINADO',]
TRANSACTION <- TRANSACTION[TRANSACTION$SKU!='COMPDEV',]

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


# Grafica para checkear

# Crear resumen de facturas faltantes por dia
datos <- FacturasSAP[FacturasSAP$estaEnOnebeat==FALSE,]
datos <- datos %>% group_by(FechaComprobante) %>%
  summarize(CantFacturasFaltantes = n()) %>%
  as.data.frame(.)
print(datos)


# Directorio
setwd("//UNILAMONEBEAT/InputFolder")
write.table(TRANSACTION,paste0("TRANSACTIONS_HISTORICO",fechayextension),row.names = FALSE, quote = FALSE,sep = ";")

