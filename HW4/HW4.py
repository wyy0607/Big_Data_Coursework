import email
import starbase
import os

if __name__ == "__main__":
    c = starbase.Connection(port=20550)
    t = c.table('ywl3922') 
    t.create('user', 'body', 'month')
    mypath = '/home/public/enron/'

    for folder in os.listdir(mypath):
        name_field = folder
        for file in os.listdir(mypath + folder):
                key = folder + "/" + file
                # read in email
                with open(mypath + key, 'r') as f:
                    read_data = f.read()
                
                try:
                    b = email.message_from_string(read_data)
                    month_field = b['date'].split(' ')[2]
                    body_field = b.get_payload()
                    t.insert(key,
                            {
                            'user': {'user': name_field},
                            'month': {'month': month_field},
                            'body':{'body': body_field}
                            }
                        )
                except:
                    pass    
