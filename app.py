import database.db
import database.parse_rss

def everyday()
    database.db.delete_old_securities(7)
    database.db.clean_data_folder()
    database.parse_rss.get_list_today()
    database.db.add_all_csv()
