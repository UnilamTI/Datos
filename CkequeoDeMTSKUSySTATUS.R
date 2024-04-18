
# Ver que combinaciones location sku de mtskus no estan en status
View( MTSKUS[!((paste0(MTSKUS$`Stock Location`,MTSKUS$SKU)) %in% paste0(STATUS$`Stock Location`,STATUS$SKU)) ,])

# Ver que combinaciones location sku de status no estan en mtskus
View( STATUS[!((paste0(STATUS$`Stock Location`,STATUS$SKU)) %in% paste0(MTSKUS$`Stock Location`,MTSKUS$SKU)) ,])

# Duplicados de STATUS
STATUS[duplicated(paste0(STATUS$`Stock Location`,STATUS$SKU)),]

# Duplicados de MTSKUS
MTSKUS[duplicated(paste0(MTSKUS$`Stock Location`,MTSKUS$SKU)),]

# Listado de Subfamilias dos duplicadas
FAMIASKA <- sqlQuery(toSAP,'SELECT * FROM (SELECT T0."ItemCode", T0."ItemName", T0."U_SEI_MODELO",  T0."U_SEI_COLOR", T0."U_SEI_TALLE", T0."U_SEI_MARCA", T0."U_SEI_DESTINO", 
               T0."U_SEI_FAMILIA",FAM."Name" AS "FAMILIA", T0."U_SEI_SUBFAMILIA1",SFAM."Name" AS "SUBFAMILIA", T0."U_SEI_SUBFAMILIA2" ,SFAM2.U_SEI_DESCRIPCION AS "SUBFAMILIA2", T0."U_SEI_NROCOMPRA",
               ROUND(T1MINO."Price"/1.22,0) AS "PrecioMinoristaSIva", ROUND(T1MAYO."Price"/1.22,0) AS "PrecioMayoristaSIva", ROUND(T1WEB."Price"/1.22,0) AS "PrecioWebSIva"
               FROM PRD_UNILAM.OITM T0 
               LEFT JOIN PRD_UNILAM.ITM1 T1MAYO ON T0."ItemCode" = T1MAYO."ItemCode" 
               AND T1MAYO."PriceList" = 2 
               LEFT JOIN PRD_UNILAM.ITM1 T1MINO ON T0."ItemCode" = T1MINO."ItemCode" 
               AND T1MINO."PriceList" = 1
               LEFT JOIN PRD_UNILAM.ITM1 T1WEB ON T0."ItemCode" = T1WEB."ItemCode" 
               AND T1WEB."PriceList" = 3
               LEFT JOIN PRD_UNILAM."@SEI_FAMILIA" FAM ON T0."U_SEI_FAMILIA"=FAM."Code"
               LEFT JOIN PRD_UNILAM."@SEI__SUBFAMILIA" SFAM ON T0."U_SEI_SUBFAMILIA1"=SFAM."Code"
               LEFT JOIN PRD_UNILAM."@SEI__SUBFAMILIA2" SFAM2 ON T0."U_SEI_SUBFAMILIA2"=SFAM2."U_SEI_CODIGO" AND T0."U_SEI_SUBFAMILIA1"=SFAM2."Code" )')

A <- FAMIASKA[,c(8,9,10,11,12,13)]
A <- A[!(is.na(A$FAMIASKA.U_SEI_SUBFAMILIA2)),]

SubFamiliasDosDuplicadas <- A[duplicated(A),]


# Que combinaciones SKU - Sucursal 

# Ver que combinaciones location sku de StocnTransitorioCZZ no estan en STATUS
View( StockEnTransitoCZZ[!((paste0(StockEnTransitoCZZ$`Stock Location`,StockEnTransitoCZZ$SKU )) %in%
                             paste0( STATUS$`Stock Location`, STATUS$SKU )) & 
                           !(StockEnTransitoCZZ$`Stock Location` %in% c('0101','0998','0104','0301','0102','0607') ),])


# Ver que articulos tienen saltos de lineas en la descripcion
MTSKUS[MTSKUS$SKU=='HT30037.00.00',]

View(MTSKUS[str_detect(MTSKUS$`SKU Description`,"\r"),2])
View(MTSKUS[str_detect(MTSKUS$`SKU Description`,'"'),])




