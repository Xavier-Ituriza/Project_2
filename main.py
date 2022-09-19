
import tkinter as tk
from tkinter import BROWSE, ttk
from ctypes import windll
from tkinter.messagebox import showinfo
import tkinter.font as font
from tkinter.scrolledtext import ScrolledText
from turtle import width
import src
import requests
import pandas as pd
import datetime
import time
import seaborn as sns
from matplotlib.backend_bases import key_press_handler
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import (
    FigureCanvasTkAgg,
    NavigationToolbar2Tk
)
import sqlalchemy as alch




windll.shcore.SetProcessDpiAwareness(1)




file_tk = open(".\Token\Token.txt")
token = file_tk.readlines()[0]
file_tk.close()



#open main window
    
w_main = tk.Tk()
w_main.title('WALL STREET HACK v.2.0')
w_main.geometry('1820x900+50+50')
w_main.resizable(False,False)
w_main.attributes('-alpha',1)
w_main.iconbitmap('./Pictures/icon.ico')  
    
f_menu = tk.Frame(w_main, width=270, height=900, bg='#4f5154')
f_menu.place(x=0, y=0)
    
        
def b_m_01():
    
    f_tickers.place(x=271, y=0)
    f_sql.place_forget()
    f_correlations.place_forget()


def b_m_02():
    
    f_sql.place(x=271, y=0)
    f_tickers.place_forget()
    f_correlations.place_forget()


def b_m_03():
    
    f_correlations.place(x=271, y=0)
    f_sql.place_forget()
    f_tickers.place_forget()
    

def on_enter_b_m(e):
    
    e.widget['background'] = '#161a21'


def on_leave_b_m(e):
    
    e.widget['background'] = '#4f5154'
    

def count_table_1():
    
    numero = src.count_registers_symbol()
    numero = '{:,}'.format(numero)
    showinfo('WALL STREET HACK v.2.0', f'Total number of registers on tickers table: {numero} registers')


def count_table_2():
    
    numero = src.count_registers_daily_trade()
    numero = '{:,}'.format(numero)
    showinfo('WALL STREET HACK v.2.0', f'Total number of registers on daily trade table: {numero} registers')
    

def count_table_3():
    
    numero = src.count_registers_daily_stats()
    numero = '{:,}'.format(numero)
    showinfo('WALL STREET HACK v.2.0', f'Total number of registers on daily stats table: {numero} registers')    


def string_to_console_sql(cadena):
    
    global console_sql
    
    console_sql.configure(state='normal')
    console_sql.insert(tk.END, cadena + '\n')
    console_sql.configure(state='disabled')
    console_sql.yview(tk.END)
    

def selecting_ticker():
    
    w_tickers = tk.Tk()
    w_tickers.title('WALL STREET HACK v.2.0')
    w_tickers.geometry('900x900+50+50')
    w_tickers.resizable(False,False)
    w_tickers.attributes('-alpha',1)
    w_tickers.iconbitmap('./Pictures/icon.ico')  
    
    t_columns = ('ticker', 'description', 'type')
    t_tickers = ttk.Treeview(w_tickers, columns=t_columns, show='headings', height=40)
    t_tickers.heading('ticker', text='TICKER', anchor='w')
    t_tickers.column('ticker', width=100)
    t_tickers.heading('description', text='NAME', anchor='w')
    t_tickers.column('description', width=250)
    t_tickers.heading('type', text='SECTOR', anchor='w')
    t_tickers.column('type', width=250)
    
    tuple_of_tickers = src.get_tuple_of_tickers()
    
    for item in tuple_of_tickers:
        t_tickers.insert('', tk.END, values=item)
    
    t_tickers.place(x=30, y=30)
    
    t_tickers_scrollbar = ttk.Scrollbar(w_tickers, orient="vertical", command=t_tickers.yview)
    t_tickers_scrollbar.place(x=635, y=25, height=830)
    
    t_tickers.configure(yscrollcommand=t_tickers_scrollbar.set)
    
    def go_selection():
        
        global tickers_list
        global console_sql
        
        console_sql.configure(state='normal')
        console_sql.delete('1.0',tk.END)
        console_sql.configure(state='disabled')
        
        tickers_list = []
        
        for selected_item in t_tickers.selection():
            paquete = []
            item = t_tickers.item(selected_item)
            paquete.append(item['values'][0])
            paquete.append(item['values'][1])
            tickers_list.append(paquete)
            paquete = []
        
        if len(tickers_list)==0:
            w_tickers.destroy()
            return
                
        w_tickers.iconify()
        
        string_to_console_sql('Starting SQL Database update process.')
        string_to_console_sql('')
        w_main.update()
        
        counter = 1
        
        for item in tickers_list:
                                   
            string_to_console_sql(f'Ticker {counter} de {len(tickers_list)}.')
            string_to_console_sql(f'{item[0]} ---> {item[1]}')
            w_main.update()
            
            bandera = False
            
            while bandera == False:
                url = 'https://www.alphavantage.co/query?function='+'TIME_SERIES_DAILY'+'&symbol='+item[0]+'&outputsize=full&apikey='+token
                r = requests.get(url)
                data_daily_trade=r.json()
                
                if list(data_daily_trade.keys())[0]!='Note':
                    string_to_console_sql(f'{item[0]} ---> getting json with historical trade data from api rest Alpha Vantage.')
                    w_main.update()
                    df_data_daily_trade = src.json_to_pandas_daily_trade(data_daily_trade, item[0])
                    string_to_console_sql(f'{item[0]} ---> transformig json to dataframe exportable to sql.')
                    w_main.update()
                    src.sql_update_from_pandas('daily_trade', df_data_daily_trade, item[0], 'NO')
                    string_to_console_sql(f'{item[0]} ---> updating dataframe to sql database, only new data will be written.')
                    w_main.update() 
                    bandera = True
                else:
                    string_to_console_sql('')
                    string_to_console_sql('Ticker fail, too much speed for a free API account. Waiting 40 seconds, and retry.')
                    string_to_console_sql('')
                    w_main.update()
                    time.sleep(40)  
            
            bandera = False   
            
            while bandera == False:
                url = 'https://www.alphavantage.co/query?function='+'SMA'+'&symbol='+item[0]+'&interval=daily&time_period='+'200'+'&series_type=close&apikey='+token
                r = requests.get(url)
                data_daily_sma200=r.json()
                
                if list(data_daily_sma200.keys())[0]!='Note':
                    string_to_console_sql(f'{item[0]} ---> getting json with simple moving average (200 days) from api rest Alpha Vantage.')
                    w_main.update()
                    df_data_daily_sma200 = src.json_to_pandas_daily_sma200(data_daily_sma200, item[0])
                    string_to_console_sql(f'{item[0]} ---> transformig json to dataframe exportable to sql.')   
                    w_main.update()
                    src.sql_update_from_pandas('daily_stats', df_data_daily_sma200, item[0], 'SMA 200')
                    string_to_console_sql(f'{item[0]} ---> updating dataframe to sql database, only new data will be written.')    
                    w_main.update()
                    bandera = True
                else:
                    string_to_console_sql('')
                    string_to_console_sql('Ticker fail, too much speed for a free API account. Waiting 40 seconds, and retry.')
                    string_to_console_sql('')
                    w_main.update()
                    time.sleep(40) 
                    
            bandera = False        

            while bandera == False:
                url = 'https://www.alphavantage.co/query?function='+'SMA'+'&symbol='+item[0]+'&interval=daily&time_period='+'50'+'&series_type=close&apikey='+token
                r = requests.get(url)
                data_daily_sma50=r.json()
            
                if list(data_daily_sma50.keys())[0]!='Note':
                    string_to_console_sql(f'{item[0]} ---> getting json with simple moving average (50 days) from api rest Alpha Vantage.')
                    w_main.update()
                    df_data_daily_sma50 = src.json_to_pandas_daily_sma50(data_daily_sma50, item[0])
                    string_to_console_sql(f'{item[0]} ---> transformig json to dataframe exportable to sql.')   
                    w_main.update()
                    src.sql_update_from_pandas('daily_stats', df_data_daily_sma50, item[0], 'SMA 50')
                    string_to_console_sql(f'{item[0]} ---> updating dataframe to sql database, only new data will be written.')    
                    string_to_console_sql('')
                    w_main.update()
                    bandera = True
                else:
                    string_to_console_sql('')
                    string_to_console_sql('Ticker fail, too much speed for a free API account. Waiting 40 seconds, and retry.')
                    string_to_console_sql('')
                    w_main.update()
                    time.sleep(40) 
           
           
            if counter == len(tickers_list):
                string_to_console_sql('Completed process, Bye!')
                w_main.update()               
           
           
            counter = counter + 1
        
                
        w_tickers.destroy()
        
        
            
    b_go = tk.Button(w_tickers, text='Proceed with selection', command=go_selection, width=20, height=1)
    b_go.place(x=670, y=30)
        
    w_tickers.mainloop()
    

    
buttonFont = font.Font(family='Calibri', size=10, weight='normal')
menu01 = tk.PhotoImage(file='./Pictures/menu01.png')
b_menu_01 = tk.Button(f_menu, text='      S&P TICKERS  ', command=b_m_01, height= 70, width=270, bg='#4f5154',fg='#ffffff', font=buttonFont, highlightthickness=0, relief='flat', image=menu01, anchor='w', compound='right')
b_menu_01.place(x=0, y=71)
    
menu02 = tk.PhotoImage(file='./Pictures/menu02.png')
b_menu_02 = tk.Button(f_menu, text='      SQL DATA FEEDER  ', command=b_m_02, height= 70, width=270, bg='#4f5154',fg='#ffffff', font=buttonFont, highlightthickness=0, relief='flat', image=menu02, anchor='w', compound='right')
b_menu_02.place(x=0, y=0)    
    
menu03 = tk.PhotoImage(file='./Pictures/menu03.png')
b_menu_03 = tk.Button(f_menu, text='      SMA 50 CORR.  ', command=b_m_03, height= 70, width=270, bg='#4f5154',fg='#ffffff', font=buttonFont, highlightthickness=0, relief='flat', image=menu03, anchor='w', compound='right')
b_menu_03.place(x=0, y=141)    
    
b_menu_01.bind("<Enter>", on_enter_b_m)
b_menu_01.bind("<Leave>", on_leave_b_m)

b_menu_02.bind("<Enter>", on_enter_b_m)
b_menu_02.bind("<Leave>", on_leave_b_m)

b_menu_03.bind("<Enter>", on_enter_b_m)
b_menu_03.bind("<Leave>", on_leave_b_m)      
    
f_tickers = tk.Frame(w_main, width=1550, height=900, bg='#F0F0F0')
f_sql = tk.Frame(w_main, width=1550, height=900, bg='#F0F0F0')
f_correlations = tk.Frame(w_main, width=1550, height=900, bg='#F0F0F0')
    
b_get_data = tk.Button(f_sql, text='  Import data from API REST ALPHA VANTAGE & export to SQL Database  ', command=selecting_ticker)
b_get_data.place(x=40, y=40)

image_b_count_table_1 = tk.PhotoImage(file='./Pictures/Table.png')
b_count_table_1 = tk.Button(f_sql, image=image_b_count_table_1, command=count_table_1, width=40, height=40)
b_count_table_1.place(x=38, y=810)
b_count_table_2 = tk.Button(f_sql, image=image_b_count_table_1, command=count_table_2, width=40, height=40)
b_count_table_2.place(x=100, y=810)
b_count_table_3 = tk.Button(f_sql, image=image_b_count_table_1, command=count_table_3, width=40, height=40)
b_count_table_3.place(x=160, y=810)

console_sql = ScrolledText(f_sql, width=120, height=29, bg='#F0F0F0')
console_sql.place(x=40, y=125)
console_sql.configure(state='disabled')



def selecting_ticker2():
    
    w_tickers = tk.Tk()
    w_tickers.title('WALL STREET HACK v.2.0')
    w_tickers.geometry('900x900+50+50')
    w_tickers.resizable(False,False)
    w_tickers.attributes('-alpha',1)
    w_tickers.iconbitmap('./Pictures/icon.ico')  
    
    t_columns = ('ticker', 'description', 'type')
    t_tickers = ttk.Treeview(w_tickers, columns=t_columns, show='headings', height=40,selectmode='browse')
    t_tickers.heading('ticker', text='TICKER', anchor='w')
    t_tickers.column('ticker', width=100)
    t_tickers.heading('description', text='NAME', anchor='w')
    t_tickers.column('description', width=250)
    t_tickers.heading('type', text='SECTOR', anchor='w')
    t_tickers.column('type', width=250)
    
    tuple_of_tickers = src.get_tuple_of_tickers()
    
    for item in tuple_of_tickers:
        t_tickers.insert('', tk.END, values=item)
    
    t_tickers.place(x=30, y=30)
    
    t_tickers_scrollbar = ttk.Scrollbar(w_tickers, orient="vertical", command=t_tickers.yview)
    t_tickers_scrollbar.place(x=635, y=25, height=830)
    
    t_tickers.configure(yscrollcommand=t_tickers_scrollbar.set)
    
    def go_selection():
        
        global tickers_list
                
        tickers_list = []
        
        for selected_item in t_tickers.selection():
            paquete = []
            item = t_tickers.item(selected_item)
            paquete.append(item['values'][0])
            paquete.append(item['values'][1])
            tickers_list.append(paquete)
            paquete = []    
        
        conexion = f"mysql+pymysql://root:Granada??@localhost/wall_street"
        engine = alch.create_engine(conexion)

        def consultar(q):
            return pd.read_sql(q, engine)

        df_to_draw_1 = consultar(f'SELECT ticker, date, close FROM daily_trade WHERE ticker="{tickers_list[0][0]}" ORDER BY date DESC')
        df_to_draw_2 = consultar(f'SELECT ticker, date, value FROM daily_stats WHERE ticker="{tickers_list[0][0]}" AND daily_stats.function="SMA 200" ORDER BY date DESC')
        df_to_draw_3 = consultar(f'SELECT ticker, date, value FROM daily_stats WHERE ticker="{tickers_list[0][0]}" AND daily_stats.function="SMA 50" ORDER BY date DESC')

        figure_tickers = Figure(figsize=(13.4, 6.7), dpi=75)
        ax = figure_tickers.subplots()
        sns.lineplot(data = df_to_draw_1, x = "date", y = "close", ax=ax)
        sns.lineplot(data = df_to_draw_2, x = "date", y = "value", ax=ax)
        sns.lineplot(data = df_to_draw_3, x = "date", y = "value", ax=ax)

        canvas_tickers = FigureCanvasTkAgg(figure_tickers, master=f_tickers)
        NavigationToolbar2Tk(canvas_tickers, f_tickers).place(x=20, y=840)
        canvas_tickers.draw()
        canvas_tickers.get_tk_widget().place(x=20, y=80)

        label_ticker_drawn.config(text=f'{tickers_list[0][0]} ({tickers_list[0][1]})')
        last_trade_info = consultar(f'SELECT MAX(date) FROM daily_trade WHERE ticker="{tickers_list[0][0]}"')['MAX(date)'][0]
        label_last_trade.config(text=f'DATE OF LAST TRADE STORED ON SQL DATABASE -> {last_trade_info}')

        w_tickers.destroy()
        
                    
    b_go = tk.Button(w_tickers, text='Draw Ticker', command=go_selection, width=20, height=1)
    b_go.place(x=670, y=30)
        
    w_tickers.mainloop()



def selecting_ticker3():
    
    w_tickers = tk.Tk()
    w_tickers.title('WALL STREET HACK v.2.0')
    w_tickers.geometry('900x900+50+50')
    w_tickers.resizable(False,False)
    w_tickers.attributes('-alpha',1)
    w_tickers.iconbitmap('./Pictures/icon.ico')  
    
    t_columns = ('ticker', 'description', 'type')
    t_tickers = ttk.Treeview(w_tickers, columns=t_columns, show='headings', height=40,selectmode='browse')
    t_tickers.heading('ticker', text='TICKER', anchor='w')
    t_tickers.column('ticker', width=100)
    t_tickers.heading('description', text='NAME', anchor='w')
    t_tickers.column('description', width=250)
    t_tickers.heading('type', text='SECTOR', anchor='w')
    t_tickers.column('type', width=250)
        
    tuple_of_tickers = src.get_tuple_of_tickers()
    
    for item in tuple_of_tickers:
        t_tickers.insert('', tk.END, values=item)
    
    t_tickers.place(x=30, y=30)
    
    t_tickers_scrollbar = ttk.Scrollbar(w_tickers, orient="vertical", command=t_tickers.yview)
    t_tickers_scrollbar.place(x=635, y=25, height=830)
    
    t_tickers.configure(yscrollcommand=t_tickers_scrollbar.set)
    
    def go_selection():
        
        global tickers_list
                
        tickers_list = []
        
        for selected_item in t_tickers.selection():
            paquete = []
            item = t_tickers.item(selected_item)
            paquete.append(item['values'][0])
            paquete.append(item['values'][1])
            tickers_list.append(paquete)
            paquete = []    

        for i in t_spearman.get_children():
            t_spearman.delete(i)
        
        w_main.update()

        tuple_corr, ttt = src.get_tuple_of_ticker_correlations_SMA50(tickers_list[0][0])
                
        for item in tuple_corr:
            t_spearman.insert('', tk.END, values=item)

        label_ticker_spearman.config(text=f'{tickers_list[0][0]} ({tickers_list[0][1]})')
        
        ticker_max = ttt.head(1)['ticker'].tolist()[0]
        ticker_min = ttt.tail(1)['ticker'].tolist()[0]
        ticker_ref = tickers_list[0][0]

        conexion = f"mysql+pymysql://root:Granada??@localhost/wall_street"
        engine = alch.create_engine(conexion)

        def consultar(q):
            return pd.read_sql(q, engine)

        df_trio_1 = consultar(f'SELECT ticker, date, close FROM daily_trade WHERE ticker="{ticker_ref}" AND date BETWEEN "2000-01-01" AND "2022-07-30" ORDER BY date DESC')
        df_trio_2 = consultar(f'SELECT ticker, date, close FROM daily_trade WHERE ticker="{ticker_max}" AND date BETWEEN "2000-01-01" AND "2022-07-30" ORDER BY date DESC')
        df_trio_3 = consultar(f'SELECT ticker, date, close FROM daily_trade WHERE ticker="{ticker_min}" AND date BETWEEN "2000-01-01" AND "2022-07-30" ORDER BY date DESC')

        figure_g1 = Figure(figsize=(7.1, 2.2), dpi=75)
        ax = figure_g1.subplots()
        sns.lineplot(data = df_trio_1, x = "date", y = "close", ax=ax)
        canvas_g1 = FigureCanvasTkAgg(figure_g1, master=f_correlations)
        canvas_g1.draw()
        canvas_g1.get_tk_widget().place(x=610, y=80)

        figure_g2 = Figure(figsize=(7.1, 2.2), dpi=75)
        ax = figure_g2.subplots()
        sns.lineplot(data = df_trio_2, x = "date", y = "close", ax=ax)
        canvas_g2 = FigureCanvasTkAgg(figure_g2, master=f_correlations)
        canvas_g2.draw()
        canvas_g2.get_tk_widget().place(x=610, y=330)

        figure_g3 = Figure(figsize=(7.1, 2.2), dpi=75)
        ax = figure_g3.subplots()
        sns.lineplot(data = df_trio_3, x = "date", y = "close", ax=ax)
        canvas_g3 = FigureCanvasTkAgg(figure_g3, master=f_correlations)
        canvas_g3.draw()
        canvas_g3.get_tk_widget().place(x=610, y=580)

        label_corr_1.config(text=ticker_ref)
        label_corr_2.config(text=ticker_max)
        label_corr_3.config(text=ticker_min)


        w_main.update()

        w_tickers.destroy()
        
                    
    b_go = tk.Button(w_tickers, text='Select Ticker', command=go_selection, width=20, height=1)
    b_go.place(x=670, y=30)
        
    w_tickers.mainloop()



b_draw_ticker = tk.Button(f_tickers, text='  Select TICKER  ', command=selecting_ticker2)
b_draw_ticker.place(x=40, y=20)

label_ticker_drawn = ttk.Label(f_tickers, text='', font=('Calibri',20))
label_ticker_drawn.place(x=250, y=15)

label_last_trade = ttk.Label(f_tickers, text='')
label_last_trade.place(x=950, y=850)



b_spearman = tk.Button(f_correlations, text='  Start SPEARMAN correlation study of SMA 50  ', command=selecting_ticker3)
b_spearman.place(x=40, y=20)

t_columnes = ('ticker', 'description', 'correlation')
t_spearman = ttk.Treeview(f_correlations, columns=t_columnes, show='headings', height=38,selectmode='browse')
t_spearman.heading('ticker', text='TICKER', anchor=tk.W)
t_spearman.column('ticker', width=100)
t_spearman.heading('description', text='DESCRIPTION', anchor=tk.W)
t_spearman.column('description', width=300)
t_spearman.heading('correlation', text='CORR.', anchor=tk.CENTER)
t_spearman.column('correlation', width=100, anchor=tk.E)
t_spearman.place(x=30, y=82)
    
t_spearman_scrollbar = ttk.Scrollbar(f_correlations, orient="vertical", command=t_spearman.yview)
t_spearman_scrollbar.place(x=535, y=80, height=782)
    
t_spearman.configure(yscrollcommand=t_spearman_scrollbar.set)


label_ticker_spearman = ttk.Label(f_correlations, text='', font=('Calibri',20))
label_ticker_spearman.place(x=600, y=15)

label_note_spearman = ttk.Label(f_correlations, text='Only TICKERS with trades prior 2015 are taken into account, to avoid insuficient data.')
label_note_spearman.place(x=850, y=850)

label_corr_1 = ttk.Label(f_correlations, text='')
label_corr_1.place(x=1420, y=80)

label_corr_2 = ttk.Label(f_correlations, text='')
label_corr_2.place(x=1420, y=330)

label_corr_3 = ttk.Label(f_correlations, text='')
label_corr_3.place(x=1420, y=580)


        
w_main.mainloop()
