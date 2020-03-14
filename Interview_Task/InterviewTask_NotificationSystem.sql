CREATE TABLE Mstnotificationtype 
  ( 
     Notificationtypeid TINYINT IDENTITY(1, 1) NOT NULL CONSTRAINT [PK_MstNotificationType] PRIMARY KEY, 
     Notificationtype   VARCHAR(50)--1- Mail,2-SMS 
  ) 

INSERT INTO Mstnotificationtype 
VALUES      ('Mail'), 
            ('SMS') 

CREATE TABLE MstEventdeatils 
  ( 
     Eventtypeid TINYINT IDENTITY(1, 1) NOT NULL CONSTRAINT [PK_MstEventDeatils] 
     PRIMARY KEY, 
     Eventtype   VARCHAR(100) 
  ) 

INSERT INTO Msteventdeatils 
VALUES      ('Insert'), 
            ('Update'), 
            ('Delete') 

CREATE TABLE Mstmaildetails 
  ( 
     Maildetailid TINYINT IDENTITY(1, 1) NOT NULL CONSTRAINT [PK_MstMailDetails] 
     PRIMARY KEY, 
     Eventtypeid  TINYINT, 
     Mailbody     NVARCHAR(max), 
     Mailsubject  NVARCHAR(255) 
  ) 
  
INSERT INTO Mstmaildetails 
VALUES      (1, 
             'Data Inserted', 
             'Insertion'), 
            (2, 
             'Data Updated', 
             'Updation'), 
            (3, 
             'Data Deleted', 
             'Deletion') 

CREATE TABLE Userinfo 
  ( 
     Userid   INT IDENTITY(1, 1) NOT NULL CONSTRAINT [PK_Userinfo] PRIMARY KEY, 
     Username VARCHAR(100), 
     Email    VARCHAR(max) ,
	 UserStatus tinyint
  ) 
 
INSERT INTO Userinfo 
VALUES ('Naveen','nawinamjuri@knovos.com',1),
       ('XYZ','xyz@knovos.com',1) 

CREATE TABLE Alertmapping 
  ( 
     Alertid            INT NOT NULL IDENTITY(1, 1) CONSTRAINT [PK_Alertmapping] PRIMARY KEY, 
     Userid             INT CONSTRAINT [FK_Alertmapping_UserId] FOREIGN KEY(UserId) REFERENCES UserInfo(UserId), 
     Eventtypeid        TINYINT CONSTRAINT [FK_Alertmapping_Eventtypeid] FOREIGN KEY(Eventtypeid) REFERENCES MstEventdeatils(Eventtypeid), 
     Notificationtypeid TINYINT CONSTRAINT [FK_Alertmapping_Notificationtypeid] FOREIGN KEY(Notificationtypeid) REFERENCES Mstnotificationtype(Notificationtypeid),
  ) 
  
INSERT INTO Alertmapping 
VALUES (1,1,1), (1,2,1) ,(2,1,1),(2,2,1),(2,3,1)

--Table for Notifications Queue
CREATE TABLE Notificationqueue 
  ( 
     Notificationid INT IDENTITY(1, 1) NOT NULL CONSTRAINT [PK_Notificationqueue] PRIMARY KEY, 
     Notificationtypeid TINYINT CONSTRAINT [FK_NotificationAlerts_MstNotificationType_NotificationTypeId] 
	 FOREIGN KEY(notificationtypeid) REFERENCES mstnotificationtype(Notificationtypeid), 
     Createddate        DATETIME, 
     Notificationstatus TINYINT,--1-Pending 5-Completed 
     Userid             INT, 
     Maildetailid       TINYINT, 
     Eventtypeid        TINYINT 
  ) 

GO
--Table to Track purpose of mail notifications
Create table AuditTrail
(
AuditId int NOT NULL IDENTITY(1,1) CONSTRAINT [PK_AuditTrail] PRIMARY KEY,
MailStatus tinyint,
Mailrecipients nvarchar(max),
MailBody nvarchar(max),
MailSubJect nvarchar(255),
Sentdate datetime,
ErrorMessage varchar(1000),
NotificationType varchar(50)
)

--Procedure to insert records in notificationqueue table as per event wise
--EXEC Usp_insertnotificationqueue 3

ALTER PROCEDURE Usp_Insertnotificationqueue 
(
@eventTypeId TINYINT
) 
AS 
  BEGIN 
      INSERT INTO Notificationqueue 
                  (notificationtypeid, 
                   createddate, 
                   notificationstatus, 
                   userid, 
                   maildetailid, 
                   eventtypeid) 
      SELECT nt.notificationtypeid, 
             Getdate(), 
             1, 
             am.userid, 
             maildetailid, 
             am.eventtypeid 
      FROM   alertmapping am WITH (nolock) 
             INNER JOIN msteventdeatils ed WITH (nolock) 
                     ON ed.eventtypeid = am.eventtypeid 
             INNER JOIN mstnotificationtype nt WITH (nolock) 
                     ON nt.notificationtypeid = am.notificationtypeid 
             INNER JOIN mstmaildetails md WITH (nolock) 
                     ON md.eventtypeid = ed.eventtypeid 
      WHERE  am.eventtypeid = @eventTypeId 
	  and not exists (select 1 from notificationqueue nq where nq.userid=am.userid and nq.eventtypeid=am.eventtypeid)
  END 

--Procedure to send mail Notifications as per user Subscribed event wise

CREATE PROCEDURE usp_sendnotificationmail 
( 
@EventTypeId TINYINT 
) 
AS 
  BEGIN 
    DECLARE @RetryCount   INT 
    DECLARE @Success      BIT 
    DECLARE @Email        VARCHAR(max) 
    DECLARE @MailSubject  VARCHAR(255) 
    DECLARE @MailBody     NVARCHAR(max) 
    DECLARE @ReturnCode   TINYINT 
    DECLARE @ErrorMessage VARCHAR(1000) 
    DECLARE @ErSeverity   INT 
    DECLARE @ErState      INT 

    SELECT @RetryCount = 1, 
           @Success =    0 
    WHILE @RetryCount < = 3 
    AND 
    @Success = 0 
    BEGIN 
      BEGIN try 
        BEGIN TRANSACTION 
        
		--getting pending Notifications from Notificationqueue table
		 
        SELECT      @MailSubject=.mailsubject, 
                    @MailBody=m.mailbody, 
                    @Email=   t.emailreciptients 
        FROM        dbo.notificationqueue np 
        INNER JOIN  mstmaildetails m 
        ON          m.maildetailid=np.maildetailid 
        CROSS apply 
                    ( 
                           SELECT Stuff ( 
                                  ( 
                                                  SELECT DISTINCT ',' +                 u.email
                                                  FROM            dbo.userinfo          AS u 
                                                  INNER JOIN      dbo.notificationqueue AS n 
                                                  ON              n.userid=u.userid 
                                                  WHERE           np.notificationid=n.notificationid FOR xml path('') ) ,1,1,'') AS emailreciptients ) AS t
        WHERE       np.eventtypeid=@EventTypeId 
        AND         notificationstatus=1 

        EXEC @ReturnCode =msdb.dbo.sp_send_dbmail 
		                   @profile_name = 'SQLAdmin', 
                           @recipients = @Email, 
                           @subject = @MailSubject, 
                           @body = @MailBody 

        IF @ReturnCode<>0 
        BEGIN 
          INSERT INTO dbo.audittrail 
                      ( 
                                  mailstatus , 
                                  mailrecipients, 
                                  mailbody, 
                                  mailsubject,
								  Sentdate, 
                                  errormessage,
								  NotificationType 
                      ) 
                      VALUES 
                      ( 
                                  5, 
                                  @Email, 
                                  @MailBody, 
                                  @MailSubject,
								  GETDATE(), 
                                  'Success',
								  'Mail' 
                      ) 

          --updating notifications which are completed 
          UPDATE notificationqueue 
          SET    notificationstatus=5 
          WHERE  eventtypeid=@EventTypeId 
          AND    notificationstatus=1 

        END 
        COMMIT TRANSACTION 
        SET @Success = 1 -- To exit the loop 
      END try 
      BEGIN catch 
        ROLLBACK TRANSACTION 

        SELECT @ErrorMessage=Error_message() 
         
        IF @ErrorMessage IS NOT NULL 
        BEGIN 
          SET @RetryCount = @RetryCount + 1 
        END 
        ELSE 
        BEGIN 
          SELECT @ErrorMessage = Error_message(), 
                 @ErSeverity = Error_severity(), 
                 @ErState = Error_state() RAISERROR (@ErMessage, @ErSeverity, @ErState ) 

          INSERT INTO dbo.audittrail 
                      ( 
                                  mailstatus , 
                                  mailrecipients, 
                                  mailbody, 
                                  mailsubject,
								  Sentdate,
								  errormessage,
								  NotificationType 
                      ) 
                      VALUES 
                      ( 
                                  3, 
                                  @Email, 
                                  @MailBody, 
                                  @MailSubject,
								  GETDATE(), 
                                  'Fail',
								  'Mail' 
                      ) 
        END 
      END catch 
    END
END

GO

--Reports

--1.Count By Type Year to Date(Current Year start to current Date) 
SELECT notificationtype, 
       Count(auditid) 
FROM   audittrail WITH (nolock) 
WHERE  sentdate BETWEEN Dateadd(yy, Datediff(yy, 0, Getdate()), 0) AND Getdate() 
GROUP  BY notificationtype 

--1.Count By Type Month to Date (Current Month start to current Date) 
SELECT notificationtype, 
       Count(auditid) 
FROM   audittrail WITH (nolock) 
WHERE  sentdate BETWEEN Dateadd(mm, Datediff(mm, 0, Getdate()), 0) AND Getdate() 
GROUP  BY notificationtype 

--2.Count BY status Year to Date(Current Year start to current Date) 
--MailStatus  3--Fail   5--Success 
SELECT mailstatus, 
       Count(auditid) 
FROM   audittrail WITH (nolock) 
WHERE  sentdate BETWEEN Dateadd(yy, Datediff(yy, 0, Getdate()), 0) AND Getdate() 
GROUP  BY mailstatus 

--2.Count BY status Month to Date (Current Month start to current Date) 
SELECT mailstatus, 
       Count(auditid) 
FROM   audittrail WITH (nolock) 
WHERE  sentdate BETWEEN Dateadd(mm, Datediff(mm, 0, Getdate()), 0) AND Getdate() 
GROUP  BY mailstatus 

--3.Top 5 Recipients By Type Year to Date(Current Year start to current Date) 
SELECT TOP 5 mailrecipients, 
             notificationtype 
FROM   audittrail WITH (nolock) 
WHERE  sentdate BETWEEN Dateadd(mm, Datediff(mm, 0, Getdate()), 0) AND Getdate() 
GROUP  BY mailrecipients, 
          notificationtype 

--3.Top 5 Recipients By Type Month to Date (Current Month start to current Date) 
SELECT TOP 5 mailrecipients, 
             notificationtype 
FROM   audittrail WITH (nolock) 
WHERE  sentdate BETWEEN Dateadd(mm, Datediff(mm, 0, Getdate()), 0) AND Getdate() 
GROUP  BY mailrecipients, 
          notificationtype 

