from urllib.parse import urlparse, parse_qs
from watson_developer_cloud import AssistantV1
from sqlalchemy import create_engine
from datetime import date, timedelta
from time import sleep
import datetime
import mysql.connector
import pandas as pd
import smtplib
import sys
import logging
import logging.handlers
import time

## First parameter s or l is short or long reports
## Second parameter p or d is production or development

if sys.argv[1] == 's':
    logging.basicConfig(datefmt='%d-%b-%y %H:%M:%S',level = logging.INFO, filename = time.strftime("short %Y-%m-%d.log"), format='%(asctime)s - %(process)d - %(levelname)s - %(message)s')

if sys.argv[1] == 'l':
    logging.basicConfig(datefmt='%d-%b-%y %H:%M:%S',level = logging.INFO, filename = time.strftime("long %Y-%m-%d.log"), format='%(asctime)s - %(process)d - %(levelname)s - %(message)s')

def print_out(message):
    logging.info(message)
    print(message)


class Sankey:

    def __init__(self, name, bot, fltr, table):

        self.name = name
        self.__bot = bot
        self.__fltr = fltr
        self.__table = table
        self.__ibm_assistant_json_logs = []
        self.df = pd.DataFrame
        self.max_turns = 5

        # ibm assistant credentials
        self.ctx = {
        "url": "https://gateway.watsonplatform.net/conversation/api",
        "username": "85e43b6b-cd4d-4d4a-889d-e1dfddde9c45",
        "password": "FIHCEXsoM1NC"
        }
        # mysql
        self.dsn_database = 'chatmantics'
        # chatmantics-dev-cluster.cluster-c47w61arwrgo.us-east-1.rds.amazonaws.com
        # chatmantics-production-v2-cluster.cluster-c47w61arwrgo.us-east-1.rds.amazonaws.com

        self.dsn_hostname = 'chatmantics-production-v2-cluster.cluster-c47w61arwrgo.us-east-1.rds.amazonaws.com'
        self.dsn_uid = "administrator"
        # PG7uaaeDu8ZdKNsp # Dev
        # XN6Fa25nU7PTavBY # Prod
        self.dsn_pwd = "XN6Fa25nU7PTavBY"

    def execute(self):
        self.extract_logs_from_ibm()
        self.save_ibm_logs_to_tbl()
        self.filter_invalid_msg_from_ibm_log()
        self.renumber_msg_in_ibm_log()
        self.filter_greater_then_max_turn()
        self.create_last_turn()
        self.create_path()
        self.create_exit_rate()
        self.create_message_percent()
        self.create_other_column()
        self.remove_transfer_from_other()
        self.aggregate_data()
        self.save_to_mysql()
        self.table_clean_up()

    def extract_logs_from_ibm(self):
        print_out('*** START *** name ' + self.name + ': Workspace id ' + str(self.__bot))
        print_out('date filter - ' + self.__fltr + ' - Execute..')
        assistant = AssistantV1(username=self.ctx.get('username'),
                                password=self.ctx.get('password'), version='2018-02-18', url=self.ctx.get('url'))
        response = {'pagination': 'DUMMY'}
        cursor = ''
        counter = 0
        while response['pagination']:
            counter = counter + 1
            if counter % 100 == 0 :
                print_out("Number of page " + str(counter))
            for i in range(0, 3):  # try 3 times error handling put in to deal with code 500 server side error
                try:
                    response = assistant.list_logs(workspace_id=self.__bot, filter=self.__fltr, page_limit=5000, cursor=cursor).get_result()
                except:
                    logging.exception(msg="Error sleep 30 sec then extract ibm log.. ", exc_info=str(sys.exc_info()[1]))
                    sleep(30)  # wait for 30 seconds before trying again
                    continue
                else:
                    break

            self.__ibm_assistant_json_logs.append(response['logs'])
            # The API has a limit of 40 logs per half hour to pull.
            # To get the full log You need to update the cursor variable with a new cursor pointing to the next page.
            # The cursor is fetched using the next_url variable, page calls do not count towards the 40 api limit
            if 'pagination' in response and 'next_url' in response['pagination']:
                # print('pagination.')
                p = response['pagination']['next_url']
                u = urlparse(p)
                query = parse_qs(u.query)
                cursor = query['cursor'][0]
        print_out('total number of pages extracted from IBM log ' + str(counter))

    def save_ibm_logs_to_tbl(self):
        tmp_tbl = []
        for logs in self.__ibm_assistant_json_logs:
            for elem in logs:
                _input = elem['response']['input']
                context = elem['response']['context']
                system = elem['response']['context']['system']
                text = _input.get('text')
                name = context.get('Name')
                sub_id = context.get('SubID')
                utm = context.get('utm')
                turn_count = system.get('dialog_turn_counter')
                conversation_id = context.get('conversation_id')
                vendor_id = context.get('vendorId')
                row = {
                    'date_filter': self.__fltr,
                    'vendor_id': vendor_id,
                    'workspace_id': self.__bot,
                    'conversation_id': conversation_id,
                    'message_text': text,
                    'utm': utm,
                    'sub_id': sub_id,
                    'turn': turn_count,
                    'to_dialog': name
                }
                tmp_tbl.append(row)
        self.df = pd.DataFrame( tmp_tbl, columns=['date_filter', 'vendor_id','workspace_id', 'conversation_id', 'turn', 'to_dialog', 'message_text', 'sub_id', 'utm',]).sort_values(['vendor_id','conversation_id', 'turn'], ascending=[True, True, True]).reset_index(drop=True)
        del self.__ibm_assistant_json_logs

    def filter_invalid_msg_from_ibm_log(self):
        df = self.df
        print_out("Total messages before filtering " + str(len(df)))
        print_out("Filter invalid messages, total to messages that are not null " + str(len(df[df.to_dialog.notnull()])))
        print_out("Filter invalid messages, total vendor messages that are not null " + str(len(df[df.vendor_id.notnull()])))
        print_out("Filter invalid messages, total messages that are vgwPostResponseTimeout " + str(len(df[df.message_text == 'vgwPostResponseTimeout'])))
        df = df[df.to_dialog.notnull()]
        df = df.fillna(0)
        df = df[df.message_text != 'vgwPostResponseTimeout']
        if len(df) <= 4:
            self.if_no_msg_clear_mysql_tbl()
        self.df = df

    def renumber_msg_in_ibm_log(self):
        df = self.df
        conversation = df.columns.get_loc("conversation_id")
        turn = df.columns.get_loc("turn")
        tbl = df.to_records(index=False)
        n = 0
        for r in range(len(tbl)-1):
            n = n + 1
            tbl[r][turn] = n
            if tbl[r][conversation] != tbl[r+1][conversation]:
                n = 0
        df = pd.DataFrame.from_records(tbl)
        self.df = df
        print_out('Renumbered turns')

    def filter_greater_then_max_turn(self):
        self.df = self.df[self.df.turn <= self.max_turns]
        print_out('Filtered max turns')

    def create_last_turn(self):
        df = self.df
        self.df['last_turn'] = 0
        conversation = df.columns.get_loc("conversation_id")
        last_turn = df.columns.get_loc("last_turn")
        tbl = df.to_records(index=False)
        for r in range(len(tbl)-1):
            if tbl[r+1][conversation] != tbl[r][conversation]:
                tbl[r][last_turn] = 1
        tbl[len(tbl)-1][last_turn] = 1  # always tag last row as last turn
        df = pd.DataFrame.from_records(tbl)
        self.df = df
        print_out('Last turns created')

    def create_path(self):
        df = self.df
        conversation = df.columns.get_loc("conversation_id")
        to = df.columns.get_loc("to_dialog")
        df['from_dialog'] = df.to_dialog.shift(1)
        df['from_dialog_path'] = ''
        from_path = df.columns.get_loc("from_dialog_path")
        df.loc[df.turn == 1, 'from_dialog'] = 'Start'
        df.loc[df.turn == 1, 'from_dialog_path'] = 'Start'
        tbl = df.to_records(index=False)
        for r in range(len(tbl)):
            if r != 0:  # Skip first row
                if tbl[r][conversation] == tbl[r-1][conversation]:
                    tbl[r][from_path] = tbl[r-1][from_path] + '/' + tbl[r-1][to]
        df = pd.DataFrame.from_records(tbl)
        self.df = df
        print_out('Paths created')

    def create_exit_rate(self):
        df = self.df
        df = df.groupby(['date_filter', 'vendor_id','workspace_id','turn', 'from_dialog_path', 'from_dialog', 'to_dialog', 'last_turn']).agg({'conversation_id': 'count'})
        tbl = df.to_records()
        df = pd.DataFrame.from_records(tbl)
        df['exit_rate'] = 0.00
        df.rename(columns={'conversation_id': 'message_count'}, inplace=True)
        to = df.columns.get_loc("to_dialog")
        last_turn = df.columns.get_loc("last_turn")
        message_count = df.columns.get_loc("message_count")
        _exit = df.columns.get_loc("exit_rate")
        tbl = df.to_records(index=False)
        first_row = 0
        for r in range(len(tbl)):
            if tbl[r][last_turn] == 1:
                tbl[r][_exit] = 1
                if r != first_row:
                    if tbl[r][to] == tbl[r-1][to]:
                        tbl[r][_exit] = round(tbl[r][message_count] / (tbl[r-1][message_count]+tbl[r][message_count]), 2)
        df = pd.DataFrame.from_records(tbl)
        self.df = df
        print_out('Exit rates created')

    def create_message_percent(self):
        df = self.df
        df['message_percent'] = 0.00
        tbl = df.to_records(index=False)
        message_percent = df.columns.get_loc("message_percent")
        message_count = df.columns.get_loc("message_count")
        vendor_id = df.columns.get_loc("vendor_id")
        for r in range(len(tbl)):
            if df[df.vendor_id.apply(str) + df.turn.apply(str) == str(tbl[r][vendor_id]) + '1'].sum()["message_count"] == 0:
                logging.info('** This should not happen.. ' + str(tbl[r][vendor_id]))
                tbl[r][message_percent] = 0
            else:
                tbl[r][message_percent] = tbl[r][message_count] / df[df.vendor_id.apply(str) + df.turn.apply(str) == str(tbl[r][vendor_id]) + '1'].sum()["message_count"]

        df = pd.DataFrame.from_records(tbl)
        df.sort_values(['vendor_id', 'turn', 'from_dialog_path', 'last_turn', 'message_count'], ascending=[True, True, True, True, False], inplace=True)
        self.df = df
        print_out('Message percentages created')

    def create_other_column(self):
        df = self.df
        df['to_dialog_other'] = ' '
        vendor = df.columns.get_loc("vendor_id")
        turn = df.columns.get_loc("turn")
        _from = df.columns.get_loc("from_dialog")
        to = df.columns.get_loc("to_dialog")
        last_turn = df.columns.get_loc("last_turn")
        other = df.columns.get_loc("to_dialog_other")
        tbl = df.to_records(index=False)
        n = 0
        take_top = 3
        for r in range(len(tbl)-1):
            n = n+1
            if n <= take_top:
                tbl[r][other] = tbl[r][to]
            if n > take_top:
                tbl[r][other] = 'Other'
            if tbl[r][last_turn] != tbl[r+1][last_turn] or tbl[r][_from] != tbl[r+1][_from] or tbl[r][turn] != tbl[r+1][turn] or tbl[r][vendor] != tbl[r+1][vendor]:
                n = 0
            if r+1 == len(tbl)-1:  # Last Row logic to tidy up
                if n <= 3:
                    tbl[r+1][other] = tbl[r+1][to]
                if n > 3:
                    tbl[r+1][other] = 'Other'
        df = pd.DataFrame.from_records(tbl)
        self.df = df
        print_out('Other column created')

    def remove_transfer_from_other(self):
        df = self.df
        df.loc[df.to_dialog == 'Transferred', 'to_other_dialog'] = 'Transfer'
        df.loc[df.to_dialog == 'transfer', 'to_other_dialog'] = 'Transfer'
        print_out('Transfers tagged to other column')
        self.df = df

    def aggregate_data(self):
        print('attempting to aggregate')
        df = self.df
        df = df.groupby(['date_filter', 'vendor_id', 'workspace_id', 'turn', 'from_dialog', 'from_dialog_path', 'to_dialog_other', 'last_turn']).agg({'message_count': 'sum', 'exit_rate': 'mean', 'message_percent': 'sum'})
        df = df.round(3)
        tbl = df.to_records()
        df = pd.DataFrame.from_records(tbl)
        df.sort_values(['turn', 'vendor_id', 'from_dialog_path', 'last_turn', 'message_count'], ascending=[True, True, True, True, False], inplace=True)
        self.df = df
        print_out('Other column aggregated')

    def save_to_mysql(self):
        print_out('about to attempt to save to my sql')
        df = self.df
        engine = create_engine('mysql+mysqlconnector://administrator:' + self.dsn_pwd + '@' + self.dsn_hostname + '/chatmantics')
        df.to_sql(name='tmpSankey2', con=engine, if_exists='replace', index=False)
        print_out('tmp2 table saved')

    def table_clean_up(self):
        import mysql.connector
        print_out('start table clean up ')
        db = mysql.connector.connect(
            host=self.dsn_hostname,
            user=self.dsn_uid,
            passwd=self.dsn_pwd,
            database=self.dsn_database
        )
        _cursor = db.cursor()
        sql = """DELETE FROM `chatmantics`.`""" + self.__table + """` WHERE workspace_id = '""" + self.__bot + """' """
        _cursor.execute(sql)
        db.commit()
        print_out("Delete clean up " + str(_cursor.rowcount))
        sql = """INSERT INTO `chatmantics`.`""" + self.__table + """` (`date_filter`,`vendor_id`,`workspace_id`,`turn`,`from_dialog`,`from_dialog_path`,`to_dialog_other`,`last_turn`,`exit_rate`,`message_percent`,`message_count`) SELECT `date_filter`,`vendor_id`,`workspace_id`,`turn`,`from_dialog`,`from_dialog_path`,`to_dialog_other`,`last_turn`,`exit_rate`,`message_percent`,`message_count` FROM `chatmantics`.tmpSankey2"""
        _cursor.execute(sql)
        db.commit()
        print_out("Insert Clean up " + str(_cursor.rowcount))
        sql = """DROP TABLE IF EXISTS tmpSankey2"""
        _cursor.execute(sql)
        print_out("Drop table clean up " + str(_cursor.rowcount))

    def if_no_msg_clear_mysql_tbl(self):
        print_out("There are zero messages so now attempting to delete previous entries from My-SQL for " + self.name)
        import mysql.connector
        db = mysql.connector.connect(
            host=self.dsn_hostname,
            user=self.dsn_uid,
            passwd=self.dsn_pwd,
            database=self.dsn_database
        )
        _cursor = db.cursor()
        sql = """DELETE FROM `chatmantics`.`""" + self.__table + """` WHERE workspace_id = '""" + self.__bot + """' """
        _cursor.execute(sql)
        db.commit()
        print_out("Delete my sql " + str(_cursor.rowcount))
        sys.exit(0)

def get_start_of_today():
    import datetime as dt
    now = datetime.datetime.now()
    today8am = now.replace(hour=8, minute=0, second=0, microsecond=0)
    if now > today8am:
        _time = dt.datetime.strptime('0000', '%H%M').time()
        start_today = dt.datetime.combine(dt.date.today(), _time)
        # Adjusting hours due to fact IBM API does not have timezone for bots
        start_today = start_today + timedelta(hours=8)
    if now <= today8am:
        _time = dt.datetime.strptime('0000', '%H%M').time()
        start_today = dt.datetime.combine(dt.date.today(), _time)
        # Adjusting hours due to fact IBM API does not have timezone for bots
        start_today = start_today + timedelta(hours=8) - timedelta(days=1)
    return start_today

def main():
    start_today = get_start_of_today()
    end_today = start_today + timedelta(days=1)
    start_yesterday = start_today - timedelta(1)
    start_seven_days_ago = start_today - timedelta(7)
    start_thirty_days_ago = start_today - timedelta(30)

    today_filter = 'response_timestamp>=' + start_today.strftime('%Y-%m-%dT%H:%M:%S') + ',response_timestamp<' + end_today.strftime('%Y-%m-%dT%H:%M:%S')
    yesterday_filter = 'response_timestamp>=' + start_yesterday.strftime('%Y-%m-%dT%H:%M:%S') + ',response_timestamp<' + start_today.strftime('%Y-%m-%dT%H:%M:%S')
    weekly_filter = 'response_timestamp>=' + start_seven_days_ago.strftime('%Y-%m-%dT%H:%M:%S') + ',response_timestamp<' + end_today.strftime('%Y-%m-%dT%H:%M:%S')
    monthly_filter = 'response_timestamp>=' + start_thirty_days_ago.strftime('%Y-%m-%dT%H:%M:%S') + ',response_timestamp<' + end_today.strftime('%Y-%m-%dT%H:%M:%S')

    sankeys = [Sankey('VOICE Student loan Out Today', 'adb0c9cd-93f0-4f04-9c59-03b1762d924b', today_filter, 'SankeyToday'),
               Sankey('VOICE Student loan Out Yesterday', 'adb0c9cd-93f0-4f04-9c59-03b1762d924b', yesterday_filter, 'SankeyYesterday'),
               Sankey('VOICE Student loan Out Weekly', 'adb0c9cd-93f0-4f04-9c59-03b1762d924b', weekly_filter, 'SankeyWeekly'),
               Sankey('VOICE Student loan In Today', 'b08b0a0f-505a-4a2f-a2fb-ea02c8b8f093', today_filter, 'SankeyToday'),
               Sankey('VOICE Student loan In Yesterday', 'b08b0a0f-505a-4a2f-a2fb-ea02c8b8f093', yesterday_filter, 'SankeyYesterday'),
               Sankey('VOICE Student loan In Weekly', 'b08b0a0f-505a-4a2f-a2fb-ea02c8b8f093', weekly_filter, 'SankeyWeekly'),
               Sankey('SMS Personal loan Today', '8c8e7df6-844c-4a83-98c1-c8ad3c061b74', today_filter, 'SankeyToday'),
               Sankey('SMS Personal loan Yesterday', '8c8e7df6-844c-4a83-98c1-c8ad3c061b74', yesterday_filter, 'SankeyYesterday'),
               Sankey('SMS Personal loan Weekly', '8c8e7df6-844c-4a83-98c1-c8ad3c061b74', weekly_filter, 'SankeyWeekly')]

    sankeyl = [Sankey('SMS Personal loan Monthly', '8c8e7df6-844c-4a83-98c1-c8ad3c061b74', monthly_filter, 'SankeyMonthly'),
               Sankey('VOICE Student loan Out Monthly', 'adb0c9cd-93f0-4f04-9c59-03b1762d924b', monthly_filter, 'SankeyMonthly'),
               Sankey('VOICE Student loan In Monthly', 'b08b0a0f-505a-4a2f-a2fb-ea02c8b8f093', monthly_filter, 'SankeyMonthly')]

    if sys.argv[1] == 's':
        for sankey in sankeys:
            try:
                sankey.execute()
            except:
                logging.exception(msg="Error ", exc_info=str(sys.exc_info()[1]))
                print_out(str(sys.exc_info()[1]))

    if sys.argv[1] == 'l':
        for sankey in sankeyl:
            try:
                sankey.execute()
            except:
                logging.exception(msg="Error ", exc_info=str(sys.exc_info()[1]))
                print_out(str(sys.exc_info()[1]))


if __name__ == '__main__':
    main()




