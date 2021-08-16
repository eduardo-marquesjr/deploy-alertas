# -*- coding: utf-8 -*-
from flask import Flask, jsonify, render_template, request, redirect, url_for
import mysql.connector
import pandas as pd
import numpy as np
import warnings
import time
import datetime as dt
import statsmodels.tsa.api as smtsa
from statsmodels.tsa.stattools import adfuller
from pmdarima.arima.utils import ndiffs
from statsmodels.tsa.arima_model import ARIMA
import statsmodels.api as sm 
warnings.filterwarnings('ignore')

mydb = mysql.connector.connect(
    host="10.11.36.1",
    port="3306", 
    user = 'emjunior',
    passwd = 'sIxg5LAHbH4la3bkwE49',
    database="zabbix"
) 

mycursor = mydb.cursor() 
 
def get_tabela(nome_tabela, coluna_ordem, limite_linhas = 500):
    select = "SELECT * FROM" + ' ' + nome_tabela + ' ' + "ORDER BY" + ' ' + coluna_ordem + ' ' + \
                                    "DESC" + ' ' + "LIMIT" + ' ' + limite_linhas
    mycursor.execute(select) 
    myresult = mycursor.fetchall() 
    tabela = pd.DataFrame(myresult, columns = mycursor.column_names) 
    for coluna in tabela.columns:
        for i in range(len(tabela[coluna])):
            if type(tabela[coluna][i]) == bytearray or type(tabela[coluna][i]) == bytes:
                tabela[coluna][i] = str(tabela[coluna][i], 'utf-8') 
                
    colunas_tempo = ['clock', 'lastchange', 'r_clock', 'lastaccess']
    for coluna in colunas_tempo:
        if coluna in mycursor.column_names:
            for i in range(tabela.shape[0]):
                tabela[coluna][i] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(tabela[coluna][i]))
    
    return tabela 

def get_tabela_big(nome_tabela):
    if nome_tabela == "items":
        select = "SELECT itemid, hostid, name FROM items WHERE hostid IN " \
              + str(tuple(hosts_top3)) + " ORDER BY itemid DESC LIMIT 200000"
    if nome_tabela == "trends":
        select = "SELECT itemid, clock, value_max FROM trends WHERE itemid IN " \
                    + str(tuple(array3)) + "AND " + "clock >= " \
                    + str(int(time.mktime((dt.date.today() - dt.timedelta(days = 180)).timetuple()))) 
    if nome_tabela == "trends_uint":
        select = "SELECT itemid, clock, value_max FROM trends WHERE itemid IN " \
                    + str(tuple(array3)) + "AND " + "clock >= " \
                    + str(int(time.mktime((dt.date.today() - dt.timedelta(days = 180)).timetuple()))) 
        
    mycursor.execute(select) 
    myresult = mycursor.fetchall() 
    tabela = pd.DataFrame(myresult, columns = mycursor.column_names) 
    for coluna in tabela.columns:
        for i in range(len(tabela[coluna])):
            if type(tabela[coluna][i]) == bytearray or type(tabela[coluna][i]) == bytes:
                tabela[coluna][i] = str(tabela[coluna][i], 'utf-8') 

    colunas_tempo = ['clock', 'lastchange', 'r_clock', 'lastaccess']
    for coluna in colunas_tempo:
        if coluna in mycursor.column_names:
            for i in range(tabela.shape[0]):
                tabela[coluna][i] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(tabela[coluna][i])) 
    
    return tabela

print('Importando tabelas...')
hosts = get_tabela("hosts", "hostid", "5000") 
hosts_top3 = [hosts.hostid[i] for i in range(hosts.shape[0]) 
               if (hosts.name[i].find('BAMAQ') != -1) or (hosts.name[i].find('bamaq') != -1) or 
               (hosts.name[i].find('CALTABIANO') != -1) or (hosts.name[i].find('caltabiano') != -1) or
               (hosts.name[i].find('ITAETE') != -1) or (hosts.name[i].find('itaete') != -1)] 
items = get_tabela_big("items") 
itemid_disco = [items.itemid[i] for i in range(items.shape[0]) if items.name[i].find('Espaco em uso [disco ') == 0 and items.name[i].find('%') != -1] 
itemid_disco = np.unique(itemid_disco)
array1 = itemid_disco
array2 = items.itemid[(items.name.str.contains('Memoria em uso %')) 
             | (items.name.str.contains('Utilizacao CPU %'))].unique()
array3 = np.unique(np.append(array1, array2)) 
trends = get_tabela_big("trends") 
trends_uint = get_tabela_big("trends_uint") 

print('Tratando os dados...') 
trends_final = pd.concat([trends, trends_uint]) 
trends_final.reset_index(drop = True, inplace = True) 
hosts['host'] = hosts['host'].apply(lambda x : str(x).upper()) 
hosts['name'] = hosts['name'].apply(lambda x : str(x).upper()) 
dados_hosts = pd.merge(hosts[['hostid','host','name']][hosts.hostid.isin(hosts_top3)].copy(),
                      items[items.hostid.isin(hosts_top3)].copy(),
                      on = 'hostid', how = 'left',suffixes=('_group', '_item'))  
dados_hosts = pd.merge(dados_hosts, trends_final.copy(),on = 'itemid', how = 'inner') 
dados_hosts['itemid'] = dados_hosts['itemid'].astype(int) 
dados_hosts['clock'] = pd.to_datetime(dados_hosts['clock'], format = '%Y-%m-%d %H:%M:%S')
dados_hosts['day'] = dados_hosts['clock'].dt.date
dados_hosts = dados_hosts.sort_values('clock', ascending = False) 
dados_hosts['recurso'] = np.where((dados_hosts.name_item.str.contains('CPU') 
                                   | dados_hosts.name_item.str.contains('cpu')) , 'cpu',
                                    np.where((dados_hosts.name_item.str.contains('DISCO') 
                                   | dados_hosts.name_item.str.contains('disco')), 'disco',
                                    np.where((dados_hosts.name_item.str.contains('Memoria') 
                                   | dados_hosts.name_item.str.contains('memoria')), 'memoria',
                            np.where(dados_hosts.name_item.str.contains('usuarios'), 'usuarios', None))))
dados_hosts['name_item'] = dados_hosts['name_item'].apply(lambda x : str(x).upper())   
dados_hosts['name_item'] = dados_hosts['name_item'].apply(lambda x : str(x).replace(' ', '_'))   
dados_hosts = dados_hosts.sort_values('clock') 
dados_hosts = dados_hosts.drop(['clock'], axis = 1) 
dados_hosts.drop_duplicates(inplace = True) 
dados_hosts.reset_index(drop = True, inplace = True) 
dados_hosts = dados_hosts.groupby(['hostid', 'host', 'name_group', 
                             'itemid', 'name_item', 'day', 'recurso']).max()
dados_hosts.drop_duplicates(inplace = True) 
dados_hosts.reset_index(inplace = True) 

print('Start da API...')
app = Flask(__name__) 
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False
app.config['JSON_AS_ASCII'] = False
app.secret_key = 'ccm'

@app.route('/home')
def home():
    return 'Bem vindo(a) a previsão de recursos da CCM! Empresas disponíveis: Bamaq, Caltabiano ou Itaete' 

@app.route('/<empresa>/grupos')
def grupos(empresa):
    empresa = empresa.upper() 
    grupos = np.unique(dados_hosts.name_group[dados_hosts.name_group.str.contains(empresa)].values)
    json_grupos = {'grupos' : list(grupos)}
    return jsonify(Data=json_grupos) 

@app.route('/<empresa>/<grupos>/hosts')
def host(empresa, grupos):
    hosts = np.unique(dados_hosts.host[dados_hosts.name_group == grupos].values)
    json_hosts = {'hosts' : list(hosts)}
    return jsonify(Data=json_hosts) 

@app.route('/<empresa>/<grupos>/<hosts>/metricas')
def metricas(empresa, grupos, hosts):
    lista_metricas = [dados_hosts.recurso[dados_hosts.host == hosts].unique()[n] for n in range(len(dados_hosts.recurso[dados_hosts.host == hosts].unique())) if dados_hosts.recurso[dados_hosts.host == hosts].unique()[n] != None]
    json_metrica = {'metricas' : list(lista_metricas)}
    return jsonify(Data=json_metrica)  
    
@app.route('/<empresa>/<grupos>/<hosts>/<metrica>/recursos')
def recursos(empresa, grupos, hosts, metrica):
    recursos = np.unique(dados_hosts.name_item[(dados_hosts.host == hosts) 
                                               & (dados_hosts.recurso == metrica)].values) 
    json_recursos = {'recursos' : list(recursos)} 
    return jsonify(Data=json_recursos) 

@app.route('/<empresa>/<grupos>/<hosts>/<metrica>/<recursos>/alertas') 
def alertas(empresa, grupos, hosts, metrica, recursos):
    y = dados_hosts.value_max[(dados_hosts.host == hosts) & (dados_hosts.name_item == recursos)]
    x = dados_hosts.day[(dados_hosts.host == hosts) & (dados_hosts.name_item == recursos)]
    result = adfuller(y) 
    if result[1] > 0.05: # if p-value > 0.05, we'll need to find the order of differencing (d) 
        d = ndiffs(y, test='adf') 
    else:
        d = 0

    p=0
    highestCorr = 0
    for i in range(1, 20):
        cor = pd.Series.autocorr(y, lag=i)
        if(cor > highestCorr):
            highestCorr = cor
            p=i    
        
    PDQ = [(i, j, k, 7) for i in range(0, 2) for j in range(0, 2) for k in range(0, 2)]
    param_lista = []
    param_seasonal_lista = []
    aic_lista = [] 
    for q in range(0, 3): 
        for param_seasonal in PDQ:
            mod = sm.tsa.statespace.SARIMAX(y, order=(p,d,q), seasonal_order=param_seasonal)
            results = mod.fit() 
            param_lista.append((p,d,q))
            param_seasonal_lista.append(param_seasonal)
            aic_lista.append(results.aic)  

    resumo = pd.DataFrame()
    resumo['param'] = param_lista
    resumo['param_seasonal'] = param_seasonal_lista
    resumo['aic'] = aic_lista
    
    mod = sm.tsa.statespace.SARIMAX(y, order = resumo.param[resumo.aic == min(resumo.aic)].values[0],
                        seasonal_order=resumo.param_seasonal[resumo.aic == min(resumo.aic)].values[0])
    results = mod.fit() 
    previsoes = results.get_prediction()  
    forecast = results.get_forecast(steps=30) 
    
    dias_previsao = [dt.date.today() + dt.timedelta(days = k) for k in range(1, 31)] 
    print(f'SARIMAX{resumo.param[resumo.aic == min(resumo.aic)].values[0]}{resumo.param_seasonal[resumo.aic == min(resumo.aic)].values[0]}')
    serie_dias = pd.Series(index = forecast.predicted_mean.index, data = dias_previsao) 
    json_alertas = {} 
    for n in forecast.predicted_mean.index:     
        if forecast.predicted_mean[n] >= 94.89:
            json_alertas[recurso] = [serie_dias[n], 'Alerta! 95%' + 'do seu recurso será alcançado nesse dia. Entre em contato com o seu CR.']

    plot1 = {'x': list(x.values), 'y' : list(y.values)}
    plot2 = {'x': list(x.values), 'y' : list(previsoes.predicted_mean)}
    plot3 = {'x': list(dias_previsao), 'y': list(forecast.predicted_mean)}
    grafico1 = {'Title' : str(hosts) + ' ' + str(recursos),
                 'xlabel' : 'Data',
                 'ylabel' : 'Valores em (%)',
                 'legend' : ['real', 'previsão', 'forecast']}  

    plot4 = {'x': list(x[len(x) - 10:]), 'y' : list(y[len(x) - 10:])}
    plot5 = {'x': list(x[len(x) - 10:]), 'y' : list(previsoes.predicted_mean[len(x) - 10:])}
    plot6 = {'x': list(dias_previsao), 'y': list(forecast.predicted_mean)}
    grafico2 = {'Title' : str(hosts) + ' ' + str(recursos),
                 'xlabel' : 'Data',
                 'ylabel' : 'Valores em (%)',
                 'legend' : ['real', 'previsão', 'forecast']}  

    json_final = {'Plot1':plot1,'Plot2':plot2,'Plot3':plot3,
                  'Plot4':plot4,'Plot5':plot5,'Plot1':plot6,
                  'Grafico1':grafico1, 'Grafico2':grafico2, 'Alertas' : json_alertas} 
    return jsonify(Data=json_final) 

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=200)   