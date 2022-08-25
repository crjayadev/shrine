import pandas as pd
import numpy as np
from copy import deepcopy
import math
import re

keydb = {}
paytmdb = {}
THRESHOLD = 0

class logger:
  def __init__(self, filename, prefix = "", date = True ):
    self.filename = filename
    self.prefix = prefix
    self.date=""
    print ("starting ", filename, " log")
    
  def log(self, message, *vartuple):
    SEP = "\t"
    #print(self.prefix)
    if self.prefix != "":
      message = str(self.prefix) + SEP + str(message)
    if self.date:
      now = datetime.now()
      dt_string = now.strftime("%Y/%m/%d%H:%M:%S") + SEP + message
    for token in vartuple:
      if token != None:
        message = str(message) + SEP + str(token)
    #print(message)


class registration:
  def __init__(self, file, headers):
    print("Code Flow: utils - In init")
    self.attr=0
    self.readRegCSVtoTable(file, headers)
    pass 

  def readRegCSVtoTable(self, fl, headers):
    print("Code Flow: utils - Reading CSV")
    data = pd.read_csv(fl, dtype={"SL.NO":str,"STUDENTS NAME":str,"PARENTS NAME":str,"MOBILE NUM":str, "ALTERNATIVE NUM":str, "E.M@IL":str})
    reg = pd.Series(data["SL.NO"]).tolist()
    name = pd.Series(data['STUDENTS NAME']).tolist()
    parent = pd.Series(data["PARENTS NAME"]).tolist()
    mobile = pd.Series(data["MOBILE NUM"]).tolist()
    alternate = pd.Series(data["ALTERNATIVE NUM"]).tolist()
    mail = pd.Series(data["E.M@IL"]).tolist()
    #operations on series
    #add, sub, mul, div, sum, prod, mean, pow, abs, cov
    self.teachNames(reg, name, "")
    self.teachNames(reg, parent, "P_")
    self.teachMobiles(reg, mobile)
    self.teachMobiles(reg, alternate)
    self.teachEmails(reg, mail)

  def teachMobiles(self, reg, mobiles):
    for i in range(len(mobiles)):
      self.teachMobile(reg[i], mobiles[i])


  def teachMobile(self, reg_id, mobile):
    if pd.isna(reg_id) or pd.isna(mobile):
      return
#    if isinstance(reg_id, float) and math.isnan(reg_id):
#      return
#    if isinstance(mobile,float) and math.isnan(mobile):
#      return
#    elif isinstance(mobile,str):
    else:
      print(type(mobile))
      phone_str = re.sub("[^0-9,+]+", '', mobile)
      phone_list = phone_str.split(',')
      #print(phone_list)
      for phone in phone_list:
        if len(phone) > 11:
          if phone.startswith("+"):
            if phone[0:-10] != "+91":
              self.addKey(reg_id, phone[0:-10],"INT",20) 
            phone = phone[-10:]
        if len(phone) == 10:
          #print("add key ", phone)
          self.addKey(reg_id,phone,"MOB",50)
          self.addKey(reg_id,phone[-4:],"MOBL4",20)


  def teachNames(self, reg, names, type_prefix):
    for i in range(len(names)):
      #print(reg[i],names[i])
      self.teachName(reg[i],names[i],type_prefix)
 
  def teachName(self, reg_id, name, type_prefix):
    if isinstance(reg_id, float) and math.isnan(reg_id):
      return
    if isinstance(name,float) and math.isnan(name):
      return
    elif isinstance(name,str):
      type = ""
      confidence = 0
      par = name.lower().split()
      for j in range(len(par)):
        if len(par[j]) > 2:
          if type == "":
            type = type_prefix + "FNAME"
            confidence = 20
          elif type == (type_prefix + "FNAME"):
            type = type_prefix + "LNAME"
            confidence = 10 
          self.addKey(reg_id,par[j],type,confidence)
     

  def addKey(self, key, value,type, confidence):
    if key not in keydb:
      keydb[key]=[]
    val = [value,type,confidence]
    keydb[key].append(val)

  def printKeyDB(self):
    for key in keydb:
      print(key)
      for i in range(len(keydb[key])):
        pass
        print ("    " + str(keydb[key][i])) 

  def teachEmails(self, reg, emails, type_prefix="" ):
    for i in range(len(emails)):
      self.teachEmail(reg[i],emails[i],type_prefix)

  def teachEmail(self, reg_id, email, type_prefix):
    if isinstance(reg_id, float) and math.isnan(reg_id):
      return
    if isinstance(email, float) and math.isnan(email):
      return
    elif isinstance(email, str):
      type=""
      confidence = 0
      self.addKey(reg_id, email, "EMAIL", 50) 
      id = email.lower().split('@')
      ln = len(id[0])
      if ln > 7:
       ln = 7
      self.addKey(reg_id, email[0:ln],"SEMAIL", 30)	  
         
        
class paytm():
  def __init__(self, file, headers):
    print("In paytm class")
    self.transactions = {}
    self.l = logger("paytmpayments", "paytm", False)
    self.readPaytmTransactions(file, headers)

  def readPaytmTransactions(self, file, headers):
    print("In readPaytmTransactions")
    data = pd.read_csv(file)
    transaction_ids = pd.Series(data["Transaction_ID"]).tolist()
    transaction_date = pd.Series(data["Transaction_Date"]).tolist()
    nicknames = pd.Series(data["Customer_Nickname"]).tolist()
    mobiles = pd.Series(data["Payment_Mobile_Number"]).tolist()
    emails = pd.Series(data["Payment_Email_Id"]).tolist()
    amounts = pd.Series(data["Amount"]).tolist()
    cust_vpas = pd.Series(data["Customer_VPA"]).tolist()
    comments = pd.Series(data["AdditionalComments"]).tolist()
    for i in range(len(transaction_ids)):
      #transaction = [str(transaction_ids[i]),str(transaction_date[i]),str(nicknames[i]),str(mobiles[i]),str(emails[i]),str(amounts[i]),str(cust_vpas[i]),str(comments[i])]
      self.reconcileTransaction(transaction_ids[i], transaction_date[i],nicknames[i], mobiles[i], emails[i], amounts[i],cust_vpas[i],comments[i])

  def printTransactions(self):
    print(self.transactions)

  def reconcileTransaction(self, transaction_id, transaction_date, nickname, mobile, email, amount, cust_vpa, comment):
    #print(transaction_id, transaction_date, nickname, mobile, email, amount, cust_vpa, comment)
    score = 0

    ret  = self.findName(nickname)

    if ret != None:
      self.l.log(ret[0], ret[1], ret[2])
      score = score  + ret[2]
      transaction = [transaction_date,transaction_id,nickname,mobile,email,cust_vpa,comment,ret[2],amount]
      if ret[0] not in self.transactions.keys():
        self.transactions[ret[0]] = [score]
      self.transactions[ret[0]][0] = score
      self.transactions[ret[0]].append(transaction)
      #print(self.transactions)

    ret = self.findMobile(mobile)
    if ret != None:
      self.l.log(ret[0], ret[1], ret[2])
      score = score + ret[2]
      transaction = [transaction_date,transaction_id,nickname,mobile,email,cust_vpa,comment,ret[2],amount]
      if ret[0] not in self.transactions.keys():
        self.transactions[ret[0]] = [score]
      self.transactions[ret[0]][0]=score
      self.transactions[ret[0]].append(transaction)
      #print(self.transactions)

    ret = self.findEmail(email)
    if len(ret) > 0:
      match = 0
      points = 0
      for i in range(len(ret)):
        if ret[i][1] > points:
          points = ret[i][1]
          match = ret[i]
      self.l.log(match)
      print("---------",match)
      score = score + points
      transaction = [transaction_date,transaction_id,nickname,mobile,email,cust_vpa,comment,score,amount]
      if match[0] not in self.transactions.keys():
        self.transactions[match[0]] = [score]
      self.transactions[match[0]][0] = score
      self.transactions[match[0]].append(transaction)
    
    ret = self.findCustomerVPA(cust_vpa)
    if len(ret) > 0:
      match = 0
      points = 0
      for i in range(len(ret)):
        if ret[i][1] > points:
          points = ret[i][1]
          match = ret[i]
      self.l.log(match)
      print("---------",match)
      score = score + points
      transaction = [transaction_date,transaction_id,nickname,mobile,email,cust_vpa,comment,score,amount]
      if match[0] not in self.transactions.keys():
        self.transactions[match[0]] = [score]
      self.transactions[match[0]][0] = score
      self.transactions[match[0]].append(transaction)

    ret = self.findComments(comment)
    if len(ret) > 0:
      match = 0
      points = 0
      for i in range(len(ret)):
        if ret[i][1] > points:
          points = ret[i][1]
          match = ret[i]
      self.l.log(match)
      print("---------",match)
      score = score + points
      transaction = [transaction_date,transaction_id,nickname,mobile,email,cust_vpa,comment,score,amount]
      if match[0] not in self.transactions.keys():
        self.transactions[match[0]] = [score]
      self.transactions[match[0]][0] = score
      self.transactions[match[0]].append(transaction)  

    

  def findName(self, name):
    if not pd.isna(name):
      for token in name.split():
        for key in keydb:
          for i in range(len(keydb[key])):
            if self.compareStr(token, keydb[key][i][0] ):
              return [key, keydb[key][i][1],keydb[key][i][2]]
      return None

  def findMobile(self, mobile):
    if not pd.isna(mobile):
      tokens=mobile.split("****")
      for key in keydb:
        print("+***********", key)
        for i in range(len(keydb[key])):
          if self.compareStr(tokens[1], keydb[key][i][0] ):
            return [key, keydb[key][i][1],keydb[key][i][2]]
      return None

  def findEmail(self, email):
    ret_val = []
    if not pd.isna(email):
      user_name = email.split('@')[0].split("****")[0]
      #tokens = user_name.split('!|#|$|%|&|\'|*|+|-|/|=|?|^|_|`|{|\|')
      #print(tokens)
      #print(keydb)
      for key in keydb:
        elements = []
        element = []
        for i in range(len(keydb[key])):
          if self.hasStr(user_name, keydb[key][i][0] ):
            element = [keydb[key][i][0], keydb[key][i][1],keydb[key][i][2]]
            if len(elements) == 0:
              elements.append(key)
              elements.append(0)
            elements[1] = elements[1] + keydb[key][i][2]
          if len(element) > 0:
            e = deepcopy(element)
            elements.append(e)
            element.clear()
        if len(elements) > 0:
          es = deepcopy(elements)  
          ret_val.append(es)
          elements.clear()
    return ret_val

  def findCustomerVPA(self, customer_vpa):
    ret_val = []
    if not pd.isna(customer_vpa):
      vpa = customer_vpa.split('@')[0]
      for key in keydb:
        elements = []
        element = []
        for i in range(len(keydb[key])):
          print(vpa)
          print(keydb[key][i][0])
          if self.hasStr(vpa, keydb[key][i][0] ):
            element = [keydb[key][i][0], keydb[key][i][1],keydb[key][i][2]]
            if len(elements) == 0:
              elements.append(key)
              elements.append(0)
            elements[1] = elements[1] + keydb[key][i][2]
          if len(element) > 0:
            e = deepcopy(element)
            elements.append(e)
            element.clear()
        if len(elements) > 0:
          es = deepcopy(elements)  
          ret_val.append(es)
          elements.clear()
    return ret_val

  def findComments(self, comments):
    ret_val = []
    if not pd.isna(comments):
      for key in keydb:
        elements = []
        element = []
        for i in range(len(keydb[key])):
          print(comments)
          print(keydb[key][i][0])
          if self.hasStr(comments, keydb[key][i][0] ):
            element = [keydb[key][i][0], keydb[key][i][1],keydb[key][i][2]]
            if len(elements) == 0:
              elements.append(key)
              elements.append(0)
            elements[1] = elements[1] + keydb[key][i][2]
          if len(element) > 0:
            e = deepcopy(element)
            elements.append(e)
            element.clear()
        if len(elements) > 0:
          es = deepcopy(elements)  
          ret_val.append(es)
          elements.clear()
    return ret_val



  def compareStr(self, word1, word2):
    #print(word1, word2)
    if word1.lower() == word2.lower():
      return True
    return False

  def hasStr(self, word1, word2):
    print(word1, word2)
    if word1.lower().find(word2.lower()) != -1:
      return True
    return False

#f="/home/raveendra/software/tests/registration.csv"
#f="/home/crj/code/shrine_reconciliation/registration.csv"
f="registration.csv"
g=""
r = registration(f,g)
r.printKeyDB()

paytmfile = "AprilPaytm.csv"
paytmdata = paytm(paytmfile, g)
paytmdata.printTransactions()

