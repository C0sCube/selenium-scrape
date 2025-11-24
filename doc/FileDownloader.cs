using MF_JSON_Handler.Helpers;
using MySql.Data.MySqlClient;
using NLog;
using Renci.SshNet;
using Renci.SshNet.Sftp;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace MF_JSON_Handler
{
    public partial class FileDownloader : Form
    {
        SftpClient? sftp = null;
        List<string> successfullyParsedFiles = new List<string>();
        List<string> failedFiles = new List<string>();

        public FileDownloader()
        {
            InitializeComponent();
        }

        private void FileDownloader_Load(object sender, EventArgs e)
        {
            textBox_serv_ip.Text = Config.GetSettingByKey("SFTPServerIP");
            textBox_port.Text = Config.GetSettingByKey("SFTPPort");
            textBox_user_id.Text = Config.GetSettingByKey("SFTPUserName");
            textBox_password.Text = Config.GetSettingByKey("SFTPPassword");
            textBox_local_folder.Text = Config.GetSettingByKey("LocalDirectory");
            text_box_ftpfolder.Text = Config.GetSettingByKey("SFTPFolder");

            //EmailSender.SendMail("Test body mail", "Test Mail");
            //lstFailedFiles.Add("File1");
            //lstFailedFiles.Add("File2");
            //lstFailedFiles.Add("File3");

            //string htmlBody = GetFailedFilesHtmlTable();
            //EmailSender.SendMail("MF JSON Handler Failed Files", htmlBody);

            EmailSender.SendMail("Application Started", "Application Started");

            ConnectFTPServer();
            Task t1 = Task.Run(() =>
            {
                while (true)
                {
                    DownloadAndProcessJSON();

                    //if (lstFailedFiles.Count > 0)
                    //{
                    //    string htmlBody = GetFailedFilesHtmlTable();
                    //    //EmailSender.SendMail("MF JSON Handler Failed Files", htmlBody);
                    //}
                    //Task.Delay(10000);

                    if (successfullyParsedFiles.Count > 0 || failedFiles.Count > 0)
                    {
                        SendFileProcessingReportEmail();
                        LogMsg(string.Format("Mail Sent at {0}{1}", DateTime.Now, Environment.NewLine));
                        successfullyParsedFiles.Clear();
                        failedFiles.Clear();
                    }
                    Thread.Sleep(10000);
                }

            });
        }

        private void ConnectFTPServer()
        {
            try
            {
                sftp = new SftpClient(textBox_serv_ip.Text, int.Parse(textBox_port.Text),
                                                    textBox_user_id.Text, textBox_password.Text);
                sftp.Connect();
                if (sftp.IsConnected)
                {
                    label_mess.Text = "Connected";
                }
                else
                {
                    label_mess.Text = "Not Connected";
                }
            }
            catch (Exception ex)
            {
                label_mess.Text = "Not Connected";

                EmailSender.SendMail("FTP Server connection failed", "Error :" + ex.Message + "\n" + ex.StackTrace);
            }

        }

        private void DownloadAndProcessJSON()
        {

            try
            {
                bool isConnected = sftp.IsConnected;
                string remoteFolderPath = text_box_ftpfolder.Text;
                string localFolderPath = textBox_local_folder.Text;
                bool recursiveDownload = true;

                if (!isConnected)
                {
                    ConnectFTPServer();
                }
                DownloadDirectory("", sftp, remoteFolderPath, localFolderPath, recursiveDownload);

            }
            catch (Exception ex)
            {
                EmailSender.SendMail("MF JSON Handler: Error", "Error in DownloadAndProcessJSON  " + ex.StackTrace);
            }
            finally
            {
                if (sftp != null) sftp.Disconnect();
                LogMsg(string.Format("Process completed at {0}{1}", DateTime.Now, Environment.NewLine));
            }
        }

        #region DownloadFiles

        private void DownloadDirectory(string FileNameToLook, SftpClient client, string source,
                                              string destination, bool recursive = false)
        {


            if (!Directory.Exists(destination)) Directory.CreateDirectory(destination);

            LogMsg("Getting list of files from SFTP remote folder....");
            var initialRemoteList = client.ListDirectory(source).ToList<SftpFile>();
            //LogMsg("Files list received from SFTP remote folder....");

            var remoteFiles = initialRemoteList.ToDictionary(f => f.Name, f => new Dictionary<string, object>()
            { { "LastWriteTime", f.LastWriteTime }, { "LastWriteTimeUtc", f.LastWriteTimeUtc } });

            var localFiles = (new DirectoryInfo(destination)).GetFiles().ToDictionary(f => f.Name, f => f.LastWriteTimeUtc);

            var finalFiles = remoteFiles.GroupJoin(localFiles, rmt => rmt.Key, src => src.Key, (remoteItem, localItem) => new { remoteItem, localItem }).Where(f => ((f.localItem.Count() == 0) || (f.remoteItem.Key == f.localItem.FirstOrDefault().Key && f.remoteItem.Value.Where(t => t.Key == "LastWriteTimeUtc").Select(v => DateTime.Parse(v.Value.ToString())).FirstOrDefault() > f.localItem.FirstOrDefault().Value))).Select(fl => new { FileName = fl.remoteItem.Key });

            var files = initialRemoteList.Join(finalFiles, initial => initial.Name, final => final.FileName, (initialItem, finalItem) => initialItem);

            LogMsg(string.Format("Files to be downloaded - Count : {0}{1}", files.Count(), Environment.NewLine));


            files.ToList().ForEach((SftpFile file) =>
            {
                if (!file.IsDirectory && !file.IsSymbolicLink)
                {

                    DownloadFile(client, file, destination,
                        remoteFiles.Where(s => s.Key == file.Name).Select(t => t.Value).FirstOrDefault().ToDictionary(f => f.Key, f => f.Value)
                        );

                }
                else if (file.IsSymbolicLink)
                {
                    LogMsg(string.Format("Symbolic link ignored: {0}", file.FullName));
                }
                else if (file.Name != "." && file.Name != "..")
                {

                    SftpFileAttributes attrs = client.GetAttributes(file.FullName);

                    Invoke(new MethodInvoker(delegate
                    {
                        ListViewItem lvi = new ListViewItem(file.Name, 1);
                        listView_data.Items.Add(lvi);
                        toolStripProgressBar1.Maximum = (int)attrs.Size;
                    }));
                    var dir = Directory.CreateDirectory(Path.Combine(destination, file.Name));
                    if (recursive)
                    {
                        DownloadDirectory(FileNameToLook, client, file.FullName, dir.FullName, true);
                    }
                }
            });
        }

        private void DownloadFile(SftpClient client, SftpFile file, string directory,
            Dictionary<string, object> remoteFileAttr)
        {

            LogMsg(string.Format("Downloading...  {0}", file.FullName));

            SftpFileAttributes attrs = client.GetAttributes(file.FullName);

            Invoke(new MethodInvoker(delegate
            {
                ListViewItem lvi = new ListViewItem(file.Name, 0);
                lvi.SubItems.Add(attrs.Size.ToString());
                listView_data.Items.Add(lvi);
                toolStripProgressBar1.Maximum = (int)attrs.Size;
            }));


            using (Stream fileStream = File.OpenWrite(Path.Combine(directory, file.Name)))
            {
                client.DownloadFile(file.FullName, fileStream, DownloadProgresBar);
            }
            LogMsg(string.Format("Downloading... {0} complete!!", file.FullName));

            //FileInfo fi = new FileInfo(Path.Combine(directory, file.Name));

            string jsonString = client.ReadAllText(file.FullName);

            bool isProcessed = ProcessingFile(file.Name, jsonString);

            if (!isProcessed)
            {
                if (!Directory.Exists(directory + "/Failed")) Directory.CreateDirectory(directory + "/Failed");
                var fileNameWithoutExtn = file.Name.Substring(0, file.Name.IndexOf("."));
                //File.Copy(Path.Combine(directory, file.Name), string.Format("{0}/{1}/{2}", directory, "Failed", file.Name.Replace(fileNameWithoutExtn, fileNameWithoutExtn + "_" + DateTime.Now.ToString("_yyyyMMdd_HHmmss_fff")), true));
                File.Copy(Path.Combine(directory, file.Name), string.Format("{0}/{1}/{2}", directory, "Failed",
                          file.Name, true));
                File.Delete(Path.Combine(directory, file.Name));
                LogMsg(string.Format("File {0} is moved to Failed", file.Name));

                failedFiles.Add(file.Name);
            }
            else
            {
                successfullyParsedFiles.Add(file.Name);
                DeleteSFTPFile(client, file.FullName);
            }

        }

        private static bool ProcessingFile(string fileName, string jsonString)
        {

            bool isProcessed = false;
            try
            {
                string spName = string.Empty;

                LogMsg("Processing Started for :" + fileName);

                if (fileName.ToLower().Contains("_kim"))
                {
                    spName = "mf_processjson_kim";
                }
                else if (fileName.ToLower().Contains("_sid"))
                {
                    spName = "mf_processjson_sid";
                }
                else if (fileName.ToLower().Contains("_fs"))
                {
                    spName = "mf_processjson_factsheet";
                }

                using (MySqlConnection con = new MySqlConnection(Config.mfDbConnectionString))
                {
                    con.Open();

                    using (var transaction = con.BeginTransaction())
                    {
                        try
                        {
                            if (fileName.ToLower().Contains("_fs"))
                            {
                                using (MySqlCommand cmd = new MySqlCommand("mf_update_document_details_FS", con, transaction))
                                {
                                    cmd.CommandType = CommandType.StoredProcedure;
                                    cmd.Parameters.AddWithValue("xfileName", fileName);
                                    cmd.ExecuteNonQuery();
                                }
                            }
                            using (MySqlCommand cmd = new MySqlCommand(spName, con, transaction))
                            {
                                cmd.CommandType = CommandType.StoredProcedure;
                                cmd.Parameters.AddWithValue("p1", jsonString);
                                cmd.ExecuteNonQuery();
                            }
                            transaction.Commit();
                            isProcessed = true;
                            LogMsg("Processing for :" + fileName + " ended.");
                        }
                        catch (Exception err)
                        {
                            transaction.Rollback();
                            LogMsg("Processing failed for file " + fileName + ". Error :- " + err.Message + err.StackTrace);
                            EmailSender.SendMail("MF JSON Handler: Error", "Error in DownloadAndProcessJSON  " + err.StackTrace);
                        }
                    }
                }
            }
            catch (Exception err)
            {
                LogMsg("Processing failed for file " + fileName + ". Error :- " + err.Message + err.StackTrace);
                EmailSender.SendMail("MF JSON Handler: Error", "Error in DownloadAndProcessJSON  " + err.StackTrace);
            }

            return isProcessed;
        }

        public static void DeleteSFTPFile(SftpClient client, string pathRemoteFileToDelete)
        {
            try
            {
                client.DeleteFile(pathRemoteFileToDelete);
            }
            catch (Exception ex)
            {
                LogNLog.LogMsg("Error while deleting SFTP File " + ex.ToString());
            }
        }
        private void DownloadProgresBar(ulong uploaded)
        {
            // Update progress bar on foreground thread
            //toolStripProgressBar1.Value = (int)uploaded
            Invoke(new MethodInvoker(delegate
            {
                toolStripProgressBar1.Value = (int)uploaded;
            }));


        }
        #endregion

        public static void LogMsg(string msg)
        {
            LogNLog.LogMsg(msg);
        }

        private void SendFileProcessingReportEmail()
        {
            StringBuilder emailBody = new StringBuilder();

            emailBody.AppendLine("<html><body>");
            emailBody.AppendLine("<h2>File Processing Report</h2>");

            if (successfullyParsedFiles.Count > 0)
            {
                emailBody.AppendLine("<h3>Files parsed successfully</h3>");
                emailBody.AppendLine("<table border='1' style='border-collapse: collapse;'>");
                emailBody.AppendLine("<tr><th>File</th></tr>");

                foreach (string file in successfullyParsedFiles)
                {
                    emailBody.AppendLine($"<tr><td>{file}</td></tr>");
                }

                emailBody.AppendLine("</table>");
            }

            if (failedFiles.Count > 0)
            {
                emailBody.AppendLine("<h3>Files that failed processing</h3>");
                emailBody.AppendLine("<table border='1' style='border-collapse: collapse;'>");
                emailBody.AppendLine("<tr><th>File</th></tr>");

                foreach (string file in failedFiles)
                {
                    emailBody.AppendLine($"<tr><td>{file}</td></tr>");
                }

                emailBody.AppendLine("</table>");
            }

            emailBody.AppendLine("</body></html>");

            // Send email with the emailBody content
            string subject = "File Processing Report";
            string body = emailBody.ToString();

            EmailSender.SendMail(subject, body);
        }

        private void FileDownloader_FormClosed(object sender, FormClosedEventArgs e)
        {
            EmailSender.SendMail("Application Closed", "Application Closed");
        }
    }
}
