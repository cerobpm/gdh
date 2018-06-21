#' getSites (gdh.db.in package)
#'
#' Esta función descarga los sitios (features) de la base de datos hidrometeorológica, junto con las
#' definiciones de series temporales asociados a los mismos. Incluye puntos (featureType='point') y
#' areas (featureType='area'). Devuelve un data.frame cuyas filas corresponden a los sitios y las
#' columnas a las propiedades, el cual sirve como parámetro de entrada para la función extractSeriesCatalog
#' (la propiedad seriesCatalog es un JSON con la definición de las series temporales). Se puede obtener el listado
#' completo o filtrar por siteCode, featureType o recuadro espacial (north,south,east,west)
#' @param con Conexión a base de datos (creada con dbConnect)
#' @param siteCode Código del sitio (int, opcional)
#' @param featureType tipo de objeto espacial ('point' o 'area', opcional)
#' @param north coordenada norte del recuadro (opcional)
#' @param south coordenada sur del recuadro (opcional)
#' @param west coordenada oeste del recuadro (opcional)
#' @param east coordenada este del recuadro (opcional)
#' @keywords sites getSite
#' @export
#' @importFrom DBI dbDriver
#' @importFrom RPostgreSQL dbConnect dbGetQuery
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr filter
#' @examples
#' drv<-dbDriver("PostgreSQL")
#' con<-dbConnect(drv, user="sololectura",host='10.10.9.14',dbname='meteorology')
#' getSites(con,featureType='point',north=-20,south=-25,east=-55,west=-60)

getSites<-function(con,siteCode=NULL,featureType=NULL,north=NULL,south=NULL,east=NULL,west=NULL) {
  sites=data.frame()
  filter=''
  if(!is.null(siteCode)) {
    filter=paste(filter," and \"siteCode\"='",siteCode,"'",sep='')
  }
  if(!is.null(north) && !is.null(south) && !is.null(east) && !is.null(west)) {
    filter=paste(filter," and \"Y\">=",south," and \"Y\"<=",north," and \"X\">=",west," and \"X\"<=",east,sep='')
  }
  if(!is.null(featureType)) {
    filter=paste(filter," and \"featureType\"='",featureType,"'",sep='')
  }
  stmt = paste("select * from features_all where \"seriesCatalog\" is not null ",filter,sep='')
#  message(stmt)
  sites=dbGetQuery(con,stmt)
  if(length(sites)==0) stop("No se encontraron features para los parametros seleccionados")

  message(paste("Se cargaron ",length(sites[,1])," features. ",sep=''))
  #  message(paste(names(series),series,'\n',sep=' '))
  return(sites)
}

#' extractSeriesCatalog (gdh.db.in)
#'
#' Esta función extrae las definiciones de series temporales de la propiedad
#' seriesCatalog (JSON string) del data.frame obtenido mediante getSites.
#' Permite filtrar por siteCode, featureType, variableCode, methodID y sourceID.
#' Devuelve un data.frame con las filas como definiciones de series y columnas como sus propiedades.
#' El mismo sirve como parámetro de entrada para la función getValues
#' @param sites data.frame obtenido mediante getSites
#' @param siteCode código de sitio (optativo)
#' @param featureType tipo de objeto espacial  ('point' o 'area' , optativo)
#' @param variableCode código de variable (optativo)
#' @param methodID ID de método (optativo)
#' @param sourceID ID de fuente (optativo)
#' @keywords series seriesCatalog
#' @export
#' @examples
#' drv<-dbDriver("PostgreSQL")
#' con<-dbConnect(drv, user="sololectura",host='10.10.9.14',dbname='meteorology')
#' sites<-getSites(con,featureType='point',north=-20,south=-25,east=-55,west=-60)
#' extractSeriesCatalog(sites,variableCode=2,methodID=1)

extractSeriesCatalog<-function(sites,siteCode=NULL,featureType=NULL,variableCode=NULL,methodID=NULL,sourceID=NULL) {
  seriesCatalog=data.frame()
  for(i in 1:length(sites$seriesCatalog)) {
    seriesCatalog<-rbind(seriesCatalog,fromJSON(sites$seriesCatalog[i]))
  }
  if(!is.null(siteCode)) {
    v=siteCode
    seriesCatalog<-filter(seriesCatalog,siteCode==v)
  }
  if(!is.null(featureType)) {
    v=featureType
    seriesCatalog<-filter(seriesCatalog,featureType==v)
  }
  if(!is.null(variableCode)) {
    v=variableCode
    seriesCatalog<-filter(seriesCatalog,variableCode==v)
  }
  if(!is.null(methodID)) {
    v=methodID
    seriesCatalog<-filter(seriesCatalog,methodID==v)
  }
  if(!is.null(sourceID)) {
    v=sourceID
    seriesCatalog<-filter(seriesCatalog,sourceID==v)
  }
  return(seriesCatalog)
}

#' getValues (gdh.db.in package)
#'
#' Esta función descarga los pares de fecha y valor para las series definidas en
#' el data.frame seriesCatalog (o un vector de valores de seriesCode), de la base de datos definida en con, entre las fechas
#' startDate y endDate. Devuelve un data.frame con los registros en las filas y las
#' propiedades en las columnas
#' @param con conexión con la base de datos (creada mediante dbConnect)
#' @param seriesCatalog data.frame obtenido de la función extractSeriesCatalog, list de parámetros de una serie  o vector de valores de seriesCode
#' @param startDate fecha inicial
#' @param endDate fecha final
#' @export
#' @keywords values timeSeries
#' @examples
#' drv<-dbDriver("PostgreSQL")
#' con<-dbConnect(drv, user="sololectura",host='10.10.9.14',dbname='meteorology')
#' sites<-getSites(con,featureType='point',north=-20,south=-25,east=-55,west=-60)
#' seriesCatalog<-extractSeriesCatalog(sites,variableCode=2,methodID=1)
#' getValues(con,seriesCatalog,'2010-01-01','2015-01-01')

getValues<-function(con,seriesCatalog,startDate,endDate) {
  data=data.frame()
  if(is.data.frame(seriesCatalog)) {
    for(i in 1:length(seriesCatalog$seriesCode)) {
      statement=paste("select \"seriesCode\",\"startDate\",\"endDate\",\"value\",'",seriesCatalog$variableName[i],"' \"variableName\" from observaciones_all where \"seriesCode\"='",seriesCatalog$seriesCode[i],"' and \"startDate\">='",startDate,"' and \"startDate\" <='",endDate,"' and \"value\" is not null order by \"startDate\"",sep='')
      #      message(statement)
      data<-rbind(data,dbGetQuery(con,statement))
    }
  } else if(is.vector(seriesCatalog)) {
    for(i in 1:length(seriesCatalog)) {
      statement=paste("select observaciones_all.\"seriesCode\",\"startDate\",\"endDate\",\"value\",series_all.\"variableName\" \"variableName\" from observaciones_all,series_all where observaciones_all.\"seriesCode\"='",seriesCatalog[i],"' and observaciones_all.\"seriesCode\"=series_all.\"seriesCode\" and \"startDate\">='",startDate,"' and \"startDate\" <='",endDate,"' and \"value\" is not null order by \"startDate\"",sep='')
      #      message(statement)
      data<-rbind(data,dbGetQuery(con,statement))
    }
  }
  else if(is.list(seriesCatalog)) {
    statement=paste("select \"seriesCode\",\"startDate\",\"endDate\",\"value\",'",seriesCatalog$variableName,"' \"variableName\" from observaciones_all where \"seriesCode\"='",seriesCatalog$seriesCode,"' and \"startDate\">='",startDate,"' and \"startDate\" <='",endDate,"' and \"value\" is not null order by \"startDate\"",sep='')
    message(statement)
    data<-rbind(data,dbGetQuery(con,statement))
  }
  if(length(data)==0) stop("No se encontraron registros para los parametros seleccionados")
  message(paste("Se cargaron ",length(data[,1])," registros. ",sep=''))
  #  message(paste(names(series),series,'\n',sep=' '))
  return(data)
}

#' plotValues (gdh.db.in package)
#'
#' Esta función grafica las series temporales obtenidas mediante getValues.
#' Admite hasta dos variables (variableName) distintas, sin límite para el número
#' de series de cada variable.
#' @param values data.frame obtenido mediante getValues
#' @param seriesCatalog data.frame obtenido mediante extractSeriesCatalog (opcional)
#' @param output Archivo donde se imprima el gráfico (opcional). Por defecto abre un display x11
#' @param types vector Tipos de gráfico para cada variable ('l': línea, 'p': puntos, 's' escalones), en el orden de aparición (opcional)
#' @param width Ancho del gráfico (opcional)
#' @param height Altura del gráfico (opcional)
#' @param invert vector de boolean Determina si debe invertirse el eje Y para cada variable (opcional)
#' @param startDate fecha inicial (opcional)
#' @param endDate fecha final (opcional)
#' @keywords plot timeSeries values
#' @export
#' @examples
#' drv<-dbDriver("PostgreSQL")
#' con<-dbConnect(drv, user="sololectura",host='10.10.9.14',dbname='meteorology')
#' sites<-getSites(con,featureType='point',north=-20,south=-25,east=-55,west=-60)
#' seriesCatalog<-extractSeriesCatalog(sites,variableCode=2,methodID=1)
#' values<-getValues(con,seriesCatalog,'2010-01-01','2015-01-01')
#' plotValues(values,seriesCatalog,output='~/tmp/plotValues.png')

plotValues<-function(values,seriesCatalog=NULL,output=NULL,types=c('l'),width=800,height=700,invert=c(FALSE),startDate=NULL,endDate=NULL) {
  if(!is.null(output)) {
    png(output,width = width, height = height)
  } else {
    x11(width=width/72,height=height/72)
  }
  varnames=unique(values$variableName)
  if(length(varnames)>2) {
    stop('max 2 variables')
    return(1);
  }
  types=if (length(varnames) > length(types)) rep(types,length(varnames)) else types
  types=types[1:length(varnames)]
  axisides=c(2,4)
  colors=seq(1,50,1)
  invert=rep(invert,length(varnames))
  k=1
  legend=c()
  lcolors=c()
  lty=c()
  pch=c()
  for (i in 1:length(varnames)) {
    subset=values[values$variableName==varnames[i],]
    ylim = if (invert[i]) c(max(subset$value),min(subset$value)) else c(min(subset$value),max(subset$value))
    xlim= c(if (!is.null(startDate)) as.Date(startDate) else  min(as.Date(values$startDate)),if (!is.null(endDate)) as.Date(endDate) else max(as.Date(values$startDate)))
    if(i==1) {
      par(mar=c(5.5,3.8,4.5,4.1),xpd=FALSE)
      plot(0,type="n",xlim=xlim,ylim=ylim,xlab='time',axes=FALSE,ylab=varnames[i])
      v=as.numeric(axis.Date(1, seq(min(values$startDate),max(values$startDate),length.out=10), format='%y-%m-%d'))
      abline(v=v,lty=3)
    } else {
      par(new = TRUE)
      plot(0,type="n",xlim=xlim,ylim=ylim,xlab=NA,axes=FALSE,ylab=NA)
      mtext(varnames[i],side=4,line=2)
    }
    roundto = if(varnames[i]=='altura') 2 else 0
    axis(axisides[i],round(seq(min(subset$value),max(subset$value),length.out=10),roundto))
    abline(h=round(seq(min(subset$value),max(subset$value),length.out=10),roundto),lty=3)
    s_ids=unique(subset$seriesCode)
    for(j in 1:length(s_ids)) {
      subsubset <- subset[subset$seriesCode == s_ids[j],]
      if(types[i]=='p') {
        points(subsubset$value~as.Date(subsubset$startDate),type='p',pch=16,cex=0.5,xaxt = 'n', yaxt = 'n',xlab=NA,ylab=NA,col=colors[k])
      } else {
        lines(subsubset$value~as.Date(subsubset$startDate),type=types[i],pch=16,cex=0.5,xaxt = 'n', yaxt = 'n',xlab=NA,ylab=NA,col=colors[k])
      }
      lty=c(lty,if(types[i]=='l' || types[i]=='s') 1 else NA)
      pch=c(pch,if(types[i]=='p') 20 else NA)
      seriesMetadata=if(!is.null(seriesCatalog)) if(!is.null(filter(seriesCatalog,seriesCode==s_ids[j]))) filter(seriesCatalog,seriesCode==s_ids[j]) else NULL else NULL
      thisLegendElem = if(!is.null(seriesMetadata)) paste(if(!is.null(seriesMetadata$siteName)) seriesMetadata$siteName else '','(',if(!is.null(seriesMetadata$siteCode)) seriesMetadata$siteCode else '',')',sep='') else s_ids[j]
      legend=c(legend,paste(varnames[i],' ',thisLegendElem,sep=''))
      lcolors=c(lcolors,colors[k])
      k=k+1
    }
  }
  if(FALSE %in% is.na(lty)) {
    legend('top',legend,col=lcolors,lty=lty,pch=pch,horiz=TRUE)
  } else {
    legend('top',legend,col=lcolors,pch=pch,horiz=TRUE)
  }
  if(!is.null(output)) {
    dev.off()
    message(paste("Se imprimio el plot en el archivo ",output,sep=''))
  }
  return(0)
}

