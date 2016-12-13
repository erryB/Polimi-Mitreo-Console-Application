using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using System.Net.Security;
using System.IO;
using Newtonsoft.Json;
using Microsoft.Azure.Devices.Client;

namespace IoTSend
{
    class Program
    {
        static DeviceClient _deviceClient;
        static string _iotHubUri = "IoT Hub Uri in the following format: **IoTHubName**.azure-devices.net";

        static string _deviceName = "Enter here the Device Name registered in IoT Hub";
        static string _deviceKey = "Enter here the Device Key corresponding to _deviceName";

        static List<Message> _batchToSend;
        static Dictionary<string, string> _nodesThreeFour;
        static void Main(string[] args)
        {
            //CertificateFix needed because of some incompatibilities on the RPI with Mono
            CertificateFix();

            try
            {
                _deviceClient = DeviceClient.Create(_iotHubUri, new DeviceAuthenticationWithRegistrySymmetricKey(_deviceName, _deviceKey));
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            _nodesThreeFour = new Dictionary<string, string>();

            for (int i = 1; i <= Directory.GetFiles(@"./Split/").Count(); i++)
            {
                string[] _rawValues = File.ReadAllLines($"./Split/{i}-raspi.txt");

                _batchToSend = new List<Message>();

                foreach (string _rawValue in _rawValues)
                {
                    try
                    {
                        Elaborate(_rawValue);
                    }
                    catch (Exception)
                    {
                        //If there are some rows that could not be parsed, parser will ignore them
                    }
                }

                NodesThreeFourClean();

                //Batch send to IoT Hub
                _deviceClient.SendEventBatchAsync(_batchToSend).Wait();
            }
        }

        /// <summary>
        /// Elaborate does _rawValue parsing 
        /// Parsed data is then added to the list of Message _batchToSend 
        /// </summary>
        /// <param name="_rawValue">This parameter contains the single reading from each node</param>
        static void Elaborate(string _rawValue)
        {
            List<string> _firstSplitList = _rawValue.Split('#').ToList();
            Dictionary<string, dynamic> _dictionaryToSerialize = new Dictionary<string, dynamic>();

            /* 
             * Values from nodes with ID:3 and ID:4 are different from the other nodes
             * A separate dictionary is used in order to collect all the packets, containing FFT values for each axis
             * These packets are split into seven different rows
             */

            if (_firstSplitList.Contains("ID:3") || _firstSplitList.Contains("ID:4"))
            {
                double actualTS = double.Parse(_firstSplitList[0].Substring(_firstSplitList[0].IndexOf(":") + 1));
                int actualID = int.Parse(_firstSplitList[1].Substring(_firstSplitList[1].IndexOf(":") + 1));
                string actualAXIS = _firstSplitList[3];
                int actualPACKETNO = int.Parse(_firstSplitList[4]);
                string actualDATA = _firstSplitList[5].Substring(_firstSplitList[5].IndexOf(":") + 1);
                if (actualDATA.EndsWith("||"))
                {
                    actualDATA = actualDATA.Substring(0, actualDATA.Length - 1);
                }
                else if (!actualDATA.EndsWith("|"))
                {
                    actualDATA = actualDATA + "|";
                }

                bool isKeyFound = false;
                string keyValue = null;
                Dictionary<string, dynamic> values = new Dictionary<string, dynamic>();

                foreach (string key in _nodesThreeFour.Keys)
                {
                    Dictionary<string, dynamic> tempDictKey = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(key);
                    bool equalTS = actualTS < tempDictKey["TS"] + 100;
                    if (tempDictKey["ID"] == actualID && tempDictKey["AXIS"] == actualAXIS && equalTS)
                    {

                        values = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(_nodesThreeFour[key]);
                        values["PACKNO"] = actualPACKETNO;
                        values["DATA"] = values["DATA"] + actualDATA;
                        if (actualPACKETNO == 7)
                        {
                            values.Add("BAT", int.Parse(_firstSplitList[2].Substring(_firstSplitList[2].IndexOf(":") + 1)));
                            values.Add("MEAN", int.Parse(_firstSplitList[6].Substring(_firstSplitList[6].IndexOf(":") + 1)));
                            values.Add("MAX", int.Parse(_firstSplitList[7].Substring(_firstSplitList[7].IndexOf(":") + 1)));
                            values.Add("MIN", int.Parse(_firstSplitList[8].Substring(_firstSplitList[8].IndexOf(":") + 1)));
                            values.Add("SD", int.Parse(_firstSplitList[9].Substring(_firstSplitList[9].IndexOf(":") + 1)));
                        }

                        keyValue = key;
                        isKeyFound = true;
                    }
                }
                if (isKeyFound)
                {
                    _nodesThreeFour[keyValue] = JsonConvert.SerializeObject(values);
                }
                else
                {
                    Dictionary<string, dynamic> actualKey = new Dictionary<string, dynamic>();
                    actualKey.Add("TS", actualTS);
                    actualKey.Add("ID", actualID);
                    actualKey.Add("AXIS", actualAXIS);

                    Dictionary<string, dynamic> actualValues = new Dictionary<string, dynamic>();
                    actualValues.Add("PACKNO", actualPACKETNO);
                    actualValues.Add("DATA", actualDATA);

                    _nodesThreeFour.Add(JsonConvert.SerializeObject(actualKey), JsonConvert.SerializeObject(actualValues));
                }
            }
            else
            {
                //A simpler parsing is due for the remaining nodes
                foreach (var item in _firstSplitList)
                {
                    var tempList = item.Split(':').ToList();
                    _dictionaryToSerialize.Add(tempList[0], double.Parse(tempList[1]));
                }
            }

            //Dictionary is JSON serialized and then encapsulated into a Message object
            string messToSend = JsonConvert.SerializeObject(_dictionaryToSerialize);
            Message messEncoded = new Message(Encoding.ASCII.GetBytes(messToSend));

            _batchToSend.Add(messEncoded);
        }

        /// <summary>
        /// NodesThreeFourClean determines which values in _nodesThreeFour are ready to be sent to IoT Hub
        /// Some values may not be ready to send: it could happen that not all the seven packets are located in the same splitted txt
        /// </summary>
        static void NodesThreeFourClean()
        {
            List<string> _keyValuesToSend = new List<string>();

            foreach (var key in _nodesThreeFour.Keys)
            {
                if (_nodesThreeFour[key].Contains("\"PACKNO\":7"))
                {
                    _keyValuesToSend.Add(key);
                }
            }

            foreach (var key in _keyValuesToSend)
            {
                Dictionary<string, dynamic> dict1 = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(key);
                Dictionary<string, dynamic> dict2 = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(_nodesThreeFour[key]);
                dict2.ToList().ForEach(x => dict1.Add(x.Key, x.Value));

                string messToSend = JsonConvert.SerializeObject(dict1);
                var messEncoded = new Message(Encoding.ASCII.GetBytes(messToSend));

                _batchToSend.Add(messEncoded);
                _nodesThreeFour.Remove(key);
            }
        }


        #region Certificate
        private static void CertificateFix()
        {
            ServicePointManager.ServerCertificateValidationCallback +=
               delegate (object sender, X509Certificate certificate,
                                  X509Chain chain,
                                  SslPolicyErrors sslPolicyErrors)
               {
                   return true;
               };
            ServicePointManager.ServerCertificateValidationCallback = MyRemoteCertificateValidationCallback;
        }

        public static bool MyRemoteCertificateValidationCallback(System.Object sender, X509Certificate certificate, X509Chain chain, SslPolicyErrors sslPolicyErrors)
        {
            bool isOk = true;
            // If there are errors in the certificate chain, look at each error to determine the cause.
            if (sslPolicyErrors != SslPolicyErrors.None)
            {
                for (int i = 0; i < chain.ChainStatus.Length; i++)
                {
                    if (chain.ChainStatus[i].Status != X509ChainStatusFlags.RevocationStatusUnknown)
                    {
                        chain.ChainPolicy.RevocationFlag = X509RevocationFlag.EntireChain;
                        chain.ChainPolicy.RevocationMode = X509RevocationMode.Online;
                        chain.ChainPolicy.UrlRetrievalTimeout = new TimeSpan(0, 1, 0);
                        chain.ChainPolicy.VerificationFlags = X509VerificationFlags.AllFlags;
                        bool chainIsValid = chain.Build((X509Certificate2)certificate);
                        if (!chainIsValid)
                        {
                            isOk = false;
                        }
                    }
                }
            }
            return isOk;
        }

        #endregion
    }
}