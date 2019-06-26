import pypyodbc as pyodbc
import operator
from datetime import datetime, timedelta
from flask import Flask
import json

app = Flask(__name__)

def connect_database():
    try:
        conn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};SERVER=tcp:nexiointranet-dev.database.windows.net;PORT=1433;' +
            'database=EventAnalyzer;UID=NexioIntranetAdmin;PWD=BarackObama#2019')
        return conn
    except Exception as e:
        print('e')
        return e


CRITICAL_PRIORITY = [8, 9, 10]
PAUSE_TIME = 300



def load_data():
    conn = connect_database()
    cursor = conn.cursor()
    users = [row for row in cursor.execute('select u.* from [tasks].[User] u;')]
    tasks = [row for row in cursor.execute(
        'select t.* from [tasks].[Task] t where t.id not in (select TaskId from [tasks].[UserTask]);')]
    assigned_tasks = [row for row in cursor.execute(
        'select * from [tasks].[UserTask] u where cast(u.start as date) = cast(getdate() as date);')]
    cursor.close()
    conn.close()
    return users, tasks, assigned_tasks


def assign_task_to_users(users, tasks, assigned_tasks_tab):
    assigned_tasks = [] + assigned_tasks_tab
    assigned_tasks_to_insert = []
    users_time = dict(((user[0], 0) for user in users))
    users_time = update_users_time(assigned_tasks, users_time)
    print(users_time)
    users_id = [user[0] for user in users]
    for task in tasks:
        task_id = task[0]
        task_time = task[3]
        if task[2] in CRITICAL_PRIORITY and assigned_tasks_tab:
            avaliables_users = check_availability(users_id, assigned_tasks, task_time)
            if avaliables_users:
                user, time_to_insert = find_closes_free_user(assigned_tasks, avaliables_users, is_critic=1)
                sql = "insert into [tasks].[UserTask] values({},{},\'{}\',\'{}\')".format(user, task_id,
                                                                                          time_to_insert + timedelta(
                                                                                              seconds=PAUSE_TIME),
                                                                                          time_to_insert + timedelta(
                                                                                              seconds=task_time + PAUSE_TIME))
                insert_critical_task(sql)
                tasks_to_update = [task for task in assigned_tasks if
                                   task[1] == user and task[0] != task_id and task[3] >= time_to_insert]
                if tasks_to_update:
                    update_time_existed_user_tasks(tasks_to_update, task_time + PAUSE_TIME)
                    assigned_tasks.append(
                        [task[0], user, task_id, time_to_insert, time_to_insert + timedelta(seconds=task_time + PAUSE_TIME)])
        else:
            avaliables_users = check_availability(users_id, assigned_tasks, task_time)
            if avaliables_users:
                user, time_to_insert = find_closes_free_user(assigned_tasks, avaliables_users, is_critic=0)
                sql = "insert into [tasks].[UserTask] values({},{},\'{}\',\'{}\')".format(user, task_id,
                                                                                          time_to_insert + timedelta(
                                                                                              seconds=PAUSE_TIME),
                                                                                          time_to_insert + timedelta(
                                                                                              seconds=task_time + PAUSE_TIME))
                insert_critical_task(sql)
                assigned_tasks.append(
                    [0, user, task_id, time_to_insert, time_to_insert + timedelta(seconds=task_time + PAUSE_TIME)])

    return assigned_tasks_to_insert


def update_users_time(assigned_tasks, users_time):
    for task in assigned_tasks:
        user_id = task[1]
        task_time = task[4] - task[3]
        users_time[user_id] += task_time.total_seconds()
    return users_time


def find_closes_free_user(assigned_tasks, users, is_critic=0):
    assigned_users = [task[1] for task in assigned_tasks]
    if is_critic:
        tasks = [[task[1], task[4], task[2]] for task in assigned_tasks if
                 task[4] > datetime.now() and task[3] < datetime.now() and task[1] in users]
    else:
        tasks = [[task[1], task[4], task[2]] for task in assigned_tasks if
                 task[4] > datetime.now() and task[1] in users]
    if assigned_tasks:
        not_used_users = [user for user in users if user not in assigned_users]
        if not_used_users:
            return not_used_users[0], datetime.now().replace(second=0, microsecond=0)
        if is_critic:
            if tasks:
                first_free_user = (min(tasks, key=lambda t: t[1]))
            else:
                return users[0], datetime.now().replace(second=0, microsecond=0)
        else:
            last_tasks = find_last_user_task(users, assigned_tasks)
            first_free_user = (min(last_tasks, key=lambda t: t[1]))
        return first_free_user[0], first_free_user[1]
    else:
        return users[0], datetime.now().replace(second=0, microsecond=0)


def find_last_user_task(users, assigned_tasks):
    last_tasks = []
    for user in users:
        user_tasks = [[task[1], task[4]] for task in assigned_tasks if task[1] == user]
        if user_tasks:
            last_tasks.append((max(user_tasks, key=lambda t: t[1])))
        else:
            last_tasks.append([user, datetime.now().replace(second=0, microsecond=0)])
    return last_tasks


def check_availability(users, assigned_tasks, task_time):
    last_tasks = find_last_user_task(users, assigned_tasks)
    avaliable_users = []
    actual_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    for user in users:
        is_avaliable = [task[0] for task in last_tasks if task[0] == user
                           and (task[1] - actual_date).total_seconds()+ task_time + PAUSE_TIME <= 64800]
        if is_avaliable:
            avaliable_users.append(is_avaliable[0])
    return avaliable_users


def insert_assigned_task_to_database(assigned_tasks):
    conn = connect_database()
    cursor = conn.cursor()
    user_tasks_start_time = dict(((user[1], 0) for user in assigned_tasks))
    for task_id, user_id, task_time in assigned_tasks:
        assign_date = datetime.now().today().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(hours=8,
                                                                                                            seconds=
                                                                                                            user_tasks_start_time[
                                                                                                                user_id])
        insert = 'INSERT INTO [tasks].[UserTask] values ({},{}, \'{}\', \'{}\');'.format(user_id, task_id,
                                                                                         str(assign_date)[:-3], str(
                (assign_date + timedelta(seconds=task_time)))[:-3])
        user_tasks_start_time[user_id] += task_time + PAUSE_TIME
        cursor.execute(insert)
        cursor.commit()


    cursor.close()
    conn.close()

def insert_critical_task(sql_command):
    conn = connect_database()
    cursor = conn.cursor()
    cursor.execute(sql_command)
    cursor.commit()
    cursor.close()
    conn.close()

def update_time_existed_user_tasks(tasks, added_time):
    conn = connect_database()
    cursor = conn.cursor()
    for task in tasks:
        update = 'Update [tasks].[UserTask] set [start] = \'{}\', [to] = \'{}\' where id = {};'.format(
            task[3] + timedelta(seconds=added_time),
            task[4] + timedelta(seconds=added_time), task[0])
        cursor.execute(update)
        cursor.commit()
    cursor.close()
    conn.close()


@app.route('/', methods=['GET'])
def home():
    return "Hello Flask TaskOptimizer"


@app.route('/optymize', methods=['GET'])
def optymize():
    users, tasks, assigned_tasks = load_data()
    sorted_by_prioryty_tasks = tasks[:]
    sorted_by_prioryty_tasks.sort(key=operator.itemgetter(2), reverse=True)
    new_assigned = assign_task_to_users(users, sorted_by_prioryty_tasks, assigned_tasks)
    insert_assigned_task_to_database(new_assigned)
    return json.dumps({'success': True}), 200, {'ContentType': 'application/json'}




if __name__ == '__main__':
    app.run()
