import pandas as pd
import numpy as np
import subprocess
import colorama
import sys

colorama.init()


class bcolors:
    OK = "\033[92m" #GREEN
    WARNING = "\033[93m" #YELLOW
    FAIL = "\033[91m" #RED
    RESET = "\033[0m" #RESET COLOR
    MSG = "\033[94m" #MENSAGE

global tresult
global bmaresult
global badsaresult
global brconstruc
global brproemsa
global bTresultf
global binresult
global bteckresult


#consolida1['DESCRIPCION']=

def telmex():
    global tresult
    consolida= pd.read_csv('C:\Actualizar_bases\CONSOLIDADO.csv',low_memory=False,encoding='latin1')#,dtype={"DESCRIPCION": str,"LAST_ACC":object,"FEC_CREACION":object})
    consolida1=consolida[consolida.SISTEMA.isin(['QAS','MEX','MTY','NTE','LDAP','CAP','DES','GDL','LAT','DEL','PPL','ADSA'])]
    consolida1.fillna({'DESCRIPCION':'SIN VALOR'})
    consolida1.DESCRIPCION.astype(str)
    dfleer = pd.read_table("C:\TELSHARE\estractor_bajas\BAJAS_PERSONAL_TELMEX_1.txt", sep='|',encoding='latin1')
    data_cols = ['EXPEDIENTE','PATERNO','MATERNO','NOMBRE','PUESTO','FECHA_ALTA','FECHA_BAJA','SUBDIVISION_PERSONAL','GRUPO_PERSONAL','FECHA_CAPTURA','ESTADO']
    dfleer.columns = data_cols
    dfleer['PATERNO']= dfleer['PATERNO'].str.strip()
    dfleer['MATERNO']= dfleer['MATERNO'].str.strip()
    dfleer['NOMBRE']= dfleer['NOMBRE'].str.strip()
    dfleer.drop(['FECHA_BAJA','FECHA_CAPTURA','PUESTO','FECHA_ALTA','SUBDIVISION_PERSONAL','GRUPO_PERSONAL'], axis=1,inplace=True)
    dfleer['EXPEDIENTE']=dfleer.EXPEDIENTE.astype('str')
    dfleer['EXPEDIENTE']=dfleer['EXPEDIENTE'].str[0:7]
    print(dfleer.EXPEDIENTE)
    tresult = consolida1.copy()
    tresult['check']=consolida1.EXPEDIENTE.isin(dfleer.EXPEDIENTE)
    tresult=tresult.loc[tresult.check==True].sort_values(by='EXPEDIENTE')
    if not tresult.empty:
        print(tresult)
    else:
        print(bcolors.FAIL + "Sin cuentas asociadas\n" + bcolors.RESET)
    mprincipal()
    
    

def amatech():
    global bmaresult
    consolida= pd.read_csv('C:\Actualizar_bases\CONSOLIDADO.csv',low_memory=False,encoding='latin1')#,dtype={"DESCRIPCION": str,"LAST_ACC":object,"FEC_CREACION":object})
    consolida1=consolida[consolida.SISTEMA.isin(['QAS','MEX','MTY','NTE','LDAP','CAP','DES','GDL','LAT','DEL','PPL','ADSA'])]
    consolida1 = consolida1.fillna(value={'DESCRIPCION':'SIN_VALOR'})
    #consolida1.DESCRIPCION.astype(str)
    dfleer = pd.read_table("C:\TELSHARE\estractor_bajas\BAJAS_PERSONAL_AMAT.txt", sep='|',encoding='latin1')
    data_cols = ['EXPEDIENTE','PATERNO','MATERNO','NOMBRE','FECHA_BAJA','FECHA_CAPTURA','PUESTO','FECHA_ALTA','SUBDIVISION_PERSONAL','GRUPO_PERSONAL']
    dfleer.columns = data_cols
    dfleer['PATERNO']= dfleer['PATERNO'].str.strip()
    dfleer['MATERNO']= dfleer['MATERNO'].str.strip()
    dfleer['NOMBRE']= dfleer['NOMBRE'].str.strip()
    dfleer.drop(['FECHA_BAJA','FECHA_CAPTURA','PUESTO','FECHA_ALTA','SUBDIVISION_PERSONAL','GRUPO_PERSONAL'], axis=1,inplace=True)
    nsepara =dfleer['NOMBRE'].str.split(expand=True)
    dfleer['NOMBRE']=dfleer['NOMBRE'].str[0:4]
    dfleer['concatenar']=dfleer['PATERNO']+ ' ' + dfleer['MATERNO'] + ' ' + dfleer['NOMBRE']
    amaresult=pd.DataFrame()
    retu2=pd.DataFrame()
    f=pd.DataFrame()
    for busca in dfleer.concatenar:
        result = consolida1[consolida1['DESCRIPCION'].str.contains(busca)]
        if not result.empty:
            amaresult=pd.concat([amaresult,result], ignore_index=False)
    if not amaresult.empty:
        amaresult.drop(['LAST_USE'],axis=1,inplace=True)
        bmaresult = consolida1.copy()
        bmaresult['check']=consolida1.USUARIO.isin(amaresult.USUARIO)
        bmaresult=bmaresult.loc[bmaresult.check==True].sort_values(by='USUARIO')
        bmaresult.drop(['LAST_USE','check'],axis=1,inplace=True)
        print(bmaresult)
    else:
        bmaresult=pd.DataFrame
        print(bcolors.FAIL + "Sin cuentas asociadas" + bcolors.RESET)
    mprincipal()
     
def adsa():
    global badsaresult
    consolida= pd.read_csv('C:\Actualizar_bases\CONSOLIDADO.csv',low_memory=False,encoding='latin1')#,dtype={"DESCRIPCION": str,"LAST_ACC":object,"FEC_CREACION":object})
    consolida1=consolida[consolida.SISTEMA.isin(['QAS','MEX','MTY','NTE','LDAP','CAP','DES','GDL','LAT','DEL','PPL','ADSA'])]
    consolida1 = consolida1.fillna(value={'DESCRIPCION':'SIN_VALOR'})
    #consolida1.DESCRIPCION.astype(str)
    dfleer = pd.read_table("C:\TELSHARE\estractor_bajas\BAJAS_PERSONAL_ADSA.txt", sep='|',encoding='latin1')
    data_cols = ['EXPEDIENTE','PATERNO','MATERNO','NOMBRE','FECHA_BAJA','FECHA_CAPTURA','PUESTO','FECHA_ALTA','SUBDIVISION_PERSONAL','GRUPO_PERSONAL']
    dfleer.columns = data_cols
    dfleer['PATERNO']= dfleer['PATERNO'].str.strip()
    dfleer['MATERNO']= dfleer['MATERNO'].str.strip()
    dfleer['NOMBRE']= dfleer['NOMBRE'].str.strip()
    dfleer.drop(['FECHA_BAJA','FECHA_CAPTURA','PUESTO','FECHA_ALTA','SUBDIVISION_PERSONAL','GRUPO_PERSONAL'], axis=1,inplace=True)
    dfleer['NOMBRE']=dfleer['NOMBRE'].str[0:4]
    dfleer['concatenar']=dfleer['PATERNO']+ ' ' + dfleer['MATERNO'] + ' ' + dfleer['NOMBRE']
    adsaresult=pd.DataFrame()
    badsaresult=pd.DataFrame()
    for busca in dfleer.concatenar:
        result = consolida1[consolida1['DESCRIPCION'].str.contains(busca)]
        if not result.empty:
            adsaresult=pd.concat([adsaresult,result], ignore_index=False)
    if not adsaresult.empty:
        adsaresult.drop(['LAST_USE'],axis=1,inplace=True)
        badsaresult = consolida1.copy()
        badsaresult['check']=consolida1.USUARIO.isin(adsaresult.USUARIO)
        badsaresult=badsaresult.loc[badsaresult.check==True].sort_values(by='USUARIO')
        print(badsaresult)
    else:
        print(bcolors.FAIL + "Sin cuentas asociadas" + bcolors.RESET)
    mprincipal()

def proemsa():
    global brproemsa
    consolida= pd.read_csv('C:\Actualizar_bases\CONSOLIDADO.csv',low_memory=False,encoding='latin1')#,dtype={"DESCRIPCION": str,"LAST_ACC":object,"FEC_CREACION":object})
    consolida1=consolida[consolida.SISTEMA.isin(['QAS','MEX','MTY','NTE','LDAP','CAP','DES','GDL','LAT','DEL','PPL','ADSA'])]
    consolida1 = consolida1.fillna(value={'DESCRIPCION':'SIN_VALOR'})
    #consolida1.DESCRIPCION.astype(str)
    dfleer = pd.read_table("C:\TELSHARE\estractor_bajas\BAJAS_PERSONAL_PROEMSA.txt", sep='|',encoding='latin1')
    data_cols = ['EXPEDIENTE','PATERNO','MATERNO','NOMBRE','FECHA_BAJA','FECHA_CAPTURA','PUESTO','FECHA_ALTA','SUBDIVISION_PERSONAL','GRUPO_PERSONAL']
    dfleer.columns = data_cols
    dfleer['PATERNO']= dfleer['PATERNO'].str.strip()
    dfleer['MATERNO']= dfleer['MATERNO'].str.strip()
    dfleer['NOMBRE']= dfleer['NOMBRE'].str.strip()
    dfleer.drop(['FECHA_BAJA','FECHA_CAPTURA','PUESTO','FECHA_ALTA','SUBDIVISION_PERSONAL','GRUPO_PERSONAL'], axis=1,inplace=True)
    dfleer['NOMBRE']=dfleer['NOMBRE'].str[0:4]
    dfleer['concatenar']=dfleer['PATERNO']+ ' ' + dfleer['MATERNO'] + ' ' + dfleer['NOMBRE']
    rproemsa=pd.DataFrame()
    brproemsa=pd.DataFrame()
    for busca in dfleer.concatenar:
        result = consolida1[consolida1['DESCRIPCION'].str.contains(busca)]
        if not result.empty:
            rproemsa=pd.concat([rproemsa,result], ignore_index=False)
    if not rproemsa.empty:
        rproemsa.drop(['LAST_USE'],axis=1,inplace=True)
        brproemsa = consolida1.copy()
        brproemsa['check']=consolida1.USUARIO.isin(rproemsa.USUARIO)
        brproemsa=brproemsa.loc[brproemsa.check==True].sort_values(by='USUARIO')
        print(brproemsa)
    else:
        print(bcolors.FAIL + "Sin cuentas asociadas" + bcolors.RESET)
    mprincipal()

def constructora():
    global brconstruc
    consolida= pd.read_csv('C:\Actualizar_bases\CONSOLIDADO.csv',low_memory=False,encoding='latin1')#,dtype={"DESCRIPCION": str,"LAST_ACC":object,"FEC_CREACION":object})
    consolida1=consolida[consolida.SISTEMA.isin(['QAS','MEX','MTY','NTE','LDAP','CAP','DES','GDL','LAT','DEL','PPL','ADSA'])]
    consolida1 = consolida1.fillna(value={'DESCRIPCION':'SIN_VALOR'})
    #consolida1.DESCRIPCION.astype(str)
    dfleer = pd.read_table("C:\TELSHARE\estractor_bajas\BAJAS_PERSONAL_CONSTRUCTORAS.txt", sep='|',encoding='latin1')
    data_cols = ['EXPEDIENTE','PATERNO','MATERNO','NOMBRE','FECHA_BAJA','FECHA_CAPTURA','PUESTO','FECHA_ALTA','SUBDIVISION_PERSONAL','GRUPO_PERSONAL']
    dfleer.columns = data_cols
    dfleer['PATERNO']= dfleer['PATERNO'].str.strip()
    dfleer['MATERNO']= dfleer['MATERNO'].str.strip()
    dfleer['NOMBRE']= dfleer['NOMBRE'].str.strip()
    dfleer.drop(['FECHA_BAJA','FECHA_CAPTURA','PUESTO','FECHA_ALTA','SUBDIVISION_PERSONAL','GRUPO_PERSONAL'], axis=1,inplace=True)
    dfleer['NOMBRE']=dfleer['NOMBRE'].str[0:4]
    dfleer['concatenar']=dfleer['PATERNO']+ ' ' + dfleer['MATERNO'] + ' ' + dfleer['NOMBRE']
    rconstruc=pd.DataFrame()
    brconstruc=pd.DataFrame()
    for busca in dfleer.concatenar:
        result = consolida1[consolida1['DESCRIPCION'].str.contains(busca)]
        if not result.empty:
            rconstruc=pd.concat([rconstruc,result], ignore_index=False)
    if not rconstruc.empty:
        rconstruc.drop(['LAST_USE'],axis=1,inplace=True)
        brconstruc = consolida1.copy()
        brconstruc['check']=consolida1.USUARIO.isin(rconstruc.USUARIO)
        brconstruc=brconstruc.loc[brconstruc.check==True].sort_values(by='USUARIO')
        print(brconstruc)
    else:
        print(bcolors.FAIL + "Sin cuentas asociadas" + bcolors.RESET)
    mprincipal()

def Telnor():
    global bTresultf
    consolida= pd.read_csv('C:\Actualizar_bases\CONSOLIDADO.csv',low_memory=False,encoding='latin1')#,dtype={"DESCRIPCION": str,"LAST_ACC":object,"FEC_CREACION":object})
    consolida1=consolida[consolida.SISTEMA.isin(['QAS','MEX','MTY','NTE','LDAP','CAP','DES','GDL','LAT','DEL','PPL','ADSA'])]
    consolida1 = consolida1.fillna(value={'DESCRIPCION':'SIN_VALOR'})
    dfleer = pd.read_table("C:\TELSHARE\estractor_bajas\BAJAS_PERSONAL_TELNOR.txt", sep='|',encoding='latin1')
    data_cols = ['EXPEDIENTE','PATERNO','MATERNO','NOMBRE','FECHA_BAJA','FECHA_CAPTURA','PUESTO','FECHA_ALTA','GRUPO_PERSONAL']        
    dfleer.columns = data_cols
    dfleer['PATERNO']= dfleer['PATERNO'].str.strip()
    dfleer['MATERNO']= dfleer['MATERNO'].str.strip()
    dfleer['NOMBRE']= dfleer['NOMBRE'].str.strip()
    dfleer.drop(['FECHA_BAJA','FECHA_CAPTURA','PUESTO','FECHA_ALTA','GRUPO_PERSONAL'], axis=1,inplace=True)
    dfleer['NOMBRE']=dfleer['NOMBRE'].str[0:4]
    dfleer['concatenar']=dfleer['PATERNO']+ ' ' + dfleer['MATERNO'] + ' ' + dfleer['NOMBRE']
    result=pd.DataFrame()
    Tresult=pd.DataFrame()
    for busca in dfleer.concatenar:
        result = consolida1[consolida1['DESCRIPCION'].str.contains(busca)]
        if not result.empty:
            Tresult=pd.concat([Tresult,result], ignore_index=False)
    if not Tresult.empty:
        Tresult.drop(['LAST_USE'],axis=1,inplace=True)
        bTresultf = consolida1.copy()
        bTresultf['check']=consolida1.USUARIO.isin(Tresult.USUARIO)
        bTresultf=bTresultf.loc[bTresultf.check==True].sort_values(by='USUARIO')
        print(bTresultf)
    else:
        print(bcolors.FAIL + "\nSin cuentas asociadas" + bcolors.RESET)
        bTresultf=pd.DataFrame()
    mprincipal()

def inmobiliarias():
    global binresult
    consolida= pd.read_csv('C:\Actualizar_bases\CONSOLIDADO.csv',low_memory=False,encoding='latin1')#,dtype={"DESCRIPCION": str,"LAST_ACC":object,"FEC_CREACION":object})
    consolida1=consolida[consolida.SISTEMA.isin(['QAS','MEX','MTY','NTE','LDAP','CAP','DES','GDL','LAT','DEL','PPL','ADSA'])]
    consolida1 = consolida1.fillna(value={'DESCRIPCION':'SIN_VALOR'})
    #consolida1.DESCRIPCION.astype(str)
    dfleer = pd.read_table("C:\TELSHARE\estractor_bajas\BAJAS_PERSONAL_INMOBILIARIAS.txt", sep='|',encoding='latin1')
    data_cols = ['EXPEDIENTE','PATERNO','MATERNO','NOMBRE','FECHA_BAJA','FECHA_CAPTURA','PUESTO','FECHA_ALTA','SUBDIVISION_PERSONAL','GRUPO_PERSONAL']
    dfleer.columns = data_cols
    dfleer['PATERNO']= dfleer['PATERNO'].str.strip()
    dfleer['MATERNO']= dfleer['MATERNO'].str.strip()
    dfleer['NOMBRE']= dfleer['NOMBRE'].str.strip()
    dfleer.drop(['FECHA_BAJA','FECHA_CAPTURA','PUESTO','FECHA_ALTA','SUBDIVISION_PERSONAL','GRUPO_PERSONAL'], axis=1,inplace=True)
    dfleer['NOMBRE']=dfleer['NOMBRE'].str[0:4]
    dfleer['concatenar']=dfleer['PATERNO']+ ' ' + dfleer['MATERNO'] + ' ' + dfleer['NOMBRE']
    result=pd.DataFrame()
    inresult=pd.DataFrame()
    binresult=pd.DataFrame()
    for busca in dfleer.concatenar:
        result = consolida1[consolida1['DESCRIPCION'].str.contains(busca)]
        if not result.empty:
             inresult=pd.concat([inresult,result], ignore_index=False)
    if not inresult.empty:
            inresult.drop(['LAST_USE'],axis=1,inplace=True)
            binresult = consolida1.copy()
            binresult['check']=consolida1.USUARIO.isin(inresult.USUARIO)
            binresult=binresult.loc[binresult.check==True].sort_values(by='USUARIO')
            print(binresult)
    else:
        print(bcolors.FAIL + "Sin cuentas asociadas" + bcolors.RESET)
    mprincipal()

def teck():
    global bteckresult
    consolida= pd.read_csv('C:\Actualizar_bases\CONSOLIDADO.csv',low_memory=False,encoding='latin1')#,dtype={"DESCRIPCION": str,"LAST_ACC":object,"FEC_CREACION":object})
    consolida1=consolida[consolida.SISTEMA.isin(['QAS','MEX','MTY','NTE','LDAP','CAP','DES','GDL','LAT','DEL','PPL','ADSA'])]
    consolida1 = consolida1.fillna(value={'DESCRIPCION':'SIN_VALOR'})
    #consolida1.DESCRIPCION.astype(str)
    dfleer = pd.read_table("C:\TELSHARE\estractor_bajas\BAJAS_PERSONAL_TCMK.txt", sep=',',encoding='latin1')
    data_cols = ['EXPEDIENTE','PATERNO','MATERNO','NOMBRE','FECHA_BAJA','FECHA_CAPTURA','PUESTO','FECHA_ALTA','SUBDIVISION_PERSONAL','GRUPO_PERSONAL']
    dfleer.columns = data_cols
    dfleer['PATERNO']= dfleer['PATERNO'].str.strip()
    dfleer['MATERNO']= dfleer['MATERNO'].str.strip()
    dfleer['NOMBRE']= dfleer['NOMBRE'].str.strip()
    dfleer.drop(['FECHA_BAJA','FECHA_CAPTURA','PUESTO','FECHA_ALTA','SUBDIVISION_PERSONAL','GRUPO_PERSONAL'], axis=1,inplace=True)
    dfleer['NOMBRE']=dfleer['NOMBRE'].str[0:4]
    dfleer['concatenar']=dfleer['PATERNO']+ ' ' + dfleer['MATERNO'] + ' ' + dfleer['NOMBRE']
    result=pd.DataFrame()
    teckresult=pd.DataFrame()
    bteckresult=pd.DataFrame()
    for busca in dfleer.concatenar:
        result = consolida1[consolida1['DESCRIPCION'].str.contains(busca)]
        if not result.empty:
             teckresult=pd.concat([teckresult,result], ignore_index=False)
    if not teckresult.empty:
            teckresult.drop(['LAST_USE'],axis=1,inplace=True)
            bteckresult = consolida1.copy()
            bteckresult['check']=consolida1.USUARIO.isin(teckresult.USUARIO)
            bteckresult=bteckresult.loc[bteckresult.check==True].sort_values(by='USUARIO')
            print(bteckresult)
    else:
        print(bcolors.FAIL + "Sin cuentas asociadas" + bcolors.RESET)
    mprincipal()


def mprincipal():
    seleccion=input('\nSelecciona la opcion desada\n1) Telmex\n2) Amatech\n3) ADSA\n4) Proemsa\n5) Constructoras\n6) Telnor\n7) Inmobiliarias\n8) Teck\n9) Unir archivos\n10) Salir\n')
    if seleccion=='1':
        telmex()
    elif seleccion=='2':
        amatech()
    elif seleccion=='3':
        adsa()
    elif seleccion=='4':
        proemsa()
    elif seleccion=='5':
        constructora()
    elif seleccion=='6':
        Telnor()
    elif seleccion=='7':
        inmobiliarias()
    elif seleccion=='8':
        teck()
    elif seleccion=='9': 
        with pd.ExcelWriter('C:\TELSHARE\estractor_bajas\consolida.xlsx') as writer:
                if not tresult.empty:
                     tresult.to_excel(writer,sheet_name='Telmex',index=False)
                if not bmaresult.empty:
                     bmaresult.to_excel(writer,sheet_name='Amatch',index=False)
                if not badsaresult.empty:
                    badsaresult.to_excel(writer,sheet_name='ADSA',index=False)
                if not brproemsa.empty:
                    brproemsa.to_excel(writer,sheet_name='Proemsa',index=False)
                if not brconstruc.empty:
                    brconstruc.to_excel(writer,sheet_name='Constructora',index=False)
                if not bTresultf.empty:
                    bTresultf.to_excel(writer,sheet_name='Telnor',index=False)
                if not binresult.empty:
                    binresult.to_excel(writer,sheet_name='inmobiliarias',index=False)
                if not bteckresult.empty:
                    bteckresult.to_excel(writer,sheet_name='teck',index=False)
        print("Archivo Generado\n")
        subprocess.Popen("C:\TELSHARE\estractor_bajas\consolida.xlsx",shell=True)

    elif seleccion=='10':
        sys.exit()
    else:
        print("\nSeleccion invalida\n")
        mprincipal()
        
mprincipal()
