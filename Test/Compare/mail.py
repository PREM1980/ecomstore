'''
Created on Apr 11, 2013

@author: temp_plakshmanan
'''
import smtplib
import string

SUBJECT = "Test email from Python"
TO = "mike@mydomain.com"
FROM = "python@mydomain.com"
text = "blah blah blah"
BODY = string.join((
        "From: %s" % FROM,
        "To: %s" % TO,
        "Subject: %s" % SUBJECT ,
        "",
        text
        ), "\r\n")
server = smtplib.SMTP('10.100.112.147')
server.sendmail(FROM, [TO], BODY)
server.quit()