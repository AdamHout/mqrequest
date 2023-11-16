/*
 * mqrequest - Demo program to send requests to a remote queue and pull the replies
 * 
 * Summary:
 * mqrequest generates requests to machine E6410 consisting of a 64KB payload of MQUINT32 values. The requests are sent
 * to remote queue definition DEV.Q1 via transmission queue E6410.TRANS.QUE over channel S1558.E6410. 
 * 
 * MQ Configuration:
 * +IBM MQ Advanced for Developers on ubuntu machine S1558
 *   -Queue Manager:
 *     -QM_S1558
 * 
 *   -Queues:
 *     -DEV.Q1          - Remote queue definition
 *     -DEV.Q2          - Local queue
 *     -E6410.TRANS.QUE - Transmission queue
 * 
 *   -Channels:
 *     -DEV.APP.SVRCONN   - Server-connection
 *     -QM_S1558.QM_E6410 - Sender
 *     -QM_E6410.QM_S1558 - Receiver
 * 
 * ----------------------------------------------------------------------------------------------
 * Date       Author        Description
 * ----------------------------------------------------------------------------------------------
 * 10/28/23   A. Hout       Original source
 * ----------------------------------------------------------------------------------------------
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
//#include <time.h>
#include <cmqc.h>

#define BUFSIZE 16384                              //16384 * sizeof(unit32_t) = 65536 = 64KB payload

/*--------------------------Begin mainline processing--------------------------*/
int main(int argc, char **argv)
{
   //MQ structures
   MQCNO   cnxOpt = {MQCNO_DEFAULT};               //Connection options  
   MQCSP   cnxSec = {MQCSP_DEFAULT};               //Security parameters
   MQOD    reqDsc = {MQOD_DEFAULT};                //Request object Descriptor
   MQOD    rpyDsc = {MQOD_DEFAULT};                //Reply object Descriptor
   MQMD    msgDsc = {MQMD_DEFAULT};                //Message Descriptor
   MQPMO   putOpt = {MQPMO_DEFAULT};               //Put message options
   MQGMO   getOpt = {MQGMO_DEFAULT};               //Get message options
   
   //MQ handles and variables
   MQHCONN  hCnx;                                  //Connection handle
   MQHOBJ   hReq;                                  //Request object handle 
   MQHOBJ   hRpy;                                  //Reply object handle 
   MQLONG   opnOpt;                                //MQOPEN options  
   MQLONG   clsOpt;                                //MQCLOSE options 
   MQLONG   cmpCde;                                //MQCONNX completion code 
   MQLONG   opnCde;                                //MQOPEN completion code 
   MQLONG   resCde;                                //Reason code 
   MQLONG   msglen;                                //Message length received 
   
   //Connection literals/variables
   char *pQmg = "QM_S1558";                        //Target queue manager
   char *pReq = "DEV.Q1";                          //Request queue - output
   char *pRpy = "DEV.Q2";                          //Reply queue - input
   
   //User credential variables
   FILE *pFP;                                      //File handle pointer
   char uid[10];                                   //User ID
   char pwd[10];                                   //User password
   
   //Message variables
   MQUINT32 seed = 1;                              //Seed variable for srand()
   MQUINT32 reqBuffer[BUFSIZE];                    //MQ request message buffer
   MQUINT32 rpyBuffer[BUFSIZE];
   
   
   //-------------------------------------------------------
   //Connect to queue manager QM_S1558
   //-------------------------------------------------------
   cnxOpt.SecurityParmsPtr = &cnxSec;
   cnxOpt.Version = MQCNO_VERSION_5;
   cnxSec.AuthenticationType = MQCSP_AUTH_USER_ID_AND_PWD;
   
   pFP = fopen("/home/adam/mqusers","r");
   if (pFP == NULL){
	   fprintf(stderr, "fopen() failed in file %s at line # %d", __FILE__,__LINE__);
	   return EXIT_FAILURE;
	}
   
	int scnt = fscanf(pFP,"%s %s",uid,pwd);
	fclose(pFP);
   if (scnt < 2){
      puts("Error pulling user credentials");
      return EXIT_FAILURE;
   }
   
   cnxSec.CSPUserIdPtr = uid;                                            
   cnxSec.CSPUserIdLength = strlen(uid);
   cnxSec.CSPPasswordPtr = pwd;
   cnxSec.CSPPasswordLength = strlen(pwd);
   MQCONNX(pQmg,&cnxOpt,&hCnx,&cmpCde,&resCde);                            //Queue manager = QM_S1558
   
   if (cmpCde == MQCC_FAILED){
      printf("MQCONNX failed with reason code %d\n",resCde);
      return (int)resCde;
   }
   
   if (cmpCde == MQCC_WARNING){
     printf("MQCONNX generated a warning with reason code %d\n",resCde);
     printf("Continuing...\n");
   }
   
   //-------------------------------------------------------
   //Open DEV.Q1 for output
   //-------------------------------------------------------
   opnOpt = MQOO_OUTPUT | MQOO_FAIL_IF_QUIESCING;
   strncpy(reqDsc.ObjectName,pReq,strlen(pReq)+1);                          //Queue = DEV.Q1; strlen+1 to include the NULL              
   MQOPEN(hCnx,&reqDsc,opnOpt,&hReq,&opnCde,&resCde);
          
   if (resCde != MQRC_NONE)
      printf("MQOPEN ended with reason code %d\n",resCde);

   if (opnCde == MQCC_FAILED){
      printf("Unable to open %s queue for output\n",pRpy);
      printf("Disconnecting from %s and exiting\n",pQmg);
      MQDISC(&hCnx,&cmpCde,&resCde);
      return (int)opnCde;
   }
   
   //-------------------------------------------------------
   //Open DEV.Q2 for exclusive input
   //-------------------------------------------------------
   opnOpt = MQOO_INPUT_EXCLUSIVE | MQOO_FAIL_IF_QUIESCING;
   strncpy(rpyDsc.ObjectName,pRpy,strlen(pReq)+1);                          //Queue = DEV.Q2; strlen+1 to include the NULL              
   MQOPEN(hCnx,&rpyDsc,opnOpt,&hRpy,&opnCde,&resCde);
          
   if (resCde != MQRC_NONE)
      printf("MQOPEN ended with reason code %d\n",resCde);

   if (opnCde == MQCC_FAILED){
      printf("Unable to open %s queue for output\n",pRpy);
      printf("Disconnecting from %s and exiting\n",pQmg);
      MQDISC(&hCnx,&cmpCde,&resCde);
      exit((int)opnCde);
   }
   
   //-------------------------------------------------------
   //Set message put and get message descriptors
   //  -Identify as a request message and pass reply-to queue
   //Set put options for remote queue definition for DEV.Q1
   //  -Unique msg ID for each request
   //Set get options for local queue DEV.Q2
   //  -Use MQGMO_VERSION_2 to avoid updating message and 
   //   correlation ID's after an MQGET
   //-------------------------------------------------------
   putOpt.Options = MQPMO_NO_SYNCPOINT | MQPMO_FAIL_IF_QUIESCING;
   putOpt.Options |= MQPMO_NEW_MSG_ID;                                     //Unique MQMD.MsgId for each request
   putOpt.Options |= MQPMO_NEW_CORREL_ID;
   
   getOpt.Version = MQGMO_VERSION_2;                                       //Don't update msg and corrl ID's
   getOpt.MatchOptions = MQMO_NONE;
   getOpt.Options = MQGMO_WAIT | MQGMO_NO_SYNCPOINT; ;
   getOpt.WaitInterval = 10000;                                            //Wait up to 10 sec for a reply
   
   //----------------------------------------------------------------
   //1. Build 64KB payloads of 32-bit random unsigned integers
   //2. Send the payload as a request to remote queue DEV.Q1
   //3. Wait for a reply on local queue DEV.Q2 - timeout after 10 sec
   //4. Repeat steps 1-3 x number of times
   //----------------------------------------------------------------
   int bufLen = sizeof(reqBuffer); 
      
   for(int ctr=0;ctr<50000;ctr++){
      srand(seed++);
      for(int ctr=0;ctr<BUFSIZE;ctr++)
         reqBuffer[ctr] = rand() % 100000;
      
      msgDsc.MsgType = MQMT_REQUEST;
      strncpy(msgDsc.ReplyToQ,pRpy,strlen(pRpy)+1);                         //Reply-to queue name   
      MQPUT(hCnx,hReq,&msgDsc,&putOpt,bufLen,reqBuffer,&cmpCde,&resCde);
      if (resCde != MQRC_NONE)
         printf("\nMQPUT ended with reason code %d\n",resCde);
      
      bufLen = sizeof(rpyBuffer);
      MQGET(hCnx,hRpy,&msgDsc,&getOpt,bufLen,rpyBuffer,&msglen,&cmpCde,&resCde);
      if (resCde != MQRC_NONE){
         if (resCde == MQRC_NO_MSG_AVAILABLE)
            puts("MQGET timed out waiting for requested reply: Code 2033");
         else
            printf("MQGET returned code: %d\n",resCde);
         break;
      }
         
      if ((ctr+1) % 25 == 0){
         printf("\r%d Replies recieved",ctr+1);
         fflush(stdout);
      }
   }
   
   //-------------------------------------------------------
   //Close DEV.Q1, DEV.Q2 and QM_S1558
   //-------------------------------------------------------
   clsOpt = MQCO_NONE;
   MQCLOSE(hCnx,&hReq,clsOpt,&cmpCde,&resCde);
   if (resCde != MQRC_NONE)
      printf("MQCLOSE %s ended with reason code %d\n",pReq,resCde);
      
   MQCLOSE(hCnx,&hRpy,clsOpt,&cmpCde,&resCde);
   if (resCde != MQRC_NONE)
      printf("MQCLOSE %s ended with reason code %d\n",pRpy,resCde);
     
   //Disconnect from the queue manager
   MQDISC(&hCnx,&cmpCde,&resCde);
   if (resCde != MQRC_NONE)
      printf("MQDISC ended with reason code %d\n",resCde);
    
   puts("\nProcessing complete");      
   return EXIT_SUCCESS;
}