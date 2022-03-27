using Microsoft.Extensions.Configuration;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;

namespace YTBulkCopy
{
    public partial class frmMain : Form
    {
        public IConfiguration config = new ConfigurationBuilder()
               .SetBasePath(Directory.GetCurrentDirectory())
               .AddJsonFile("appsettings.json")
               .Build();
        string _sourceConnStr = String.Empty;
        string _descConnStr = String.Empty;

        public frmMain()
        {
            InitializeComponent();
            _sourceConnStr = config.GetConnectionString("SourceConnectString");
            _descConnStr = config.GetConnectionString("DestConnectString");
        }

        private void frmMain_Load(object sender, EventArgs e)
        {
            //*****************************************************************************************************************
            // For cannot export or attach DB from MSSQL case. Such as from windows move to linux platform / Docker container
            //*****************************************************************************************************************
        }

        public void BulkCopyYTData(String strTableName, String strCondition)
        {
            SqlConnection sourceConn = new SqlConnection(_sourceConnStr);
            SqlConnection destConn = new SqlConnection(_descConnStr);
           
            try
            {
                sourceConn.Open();
                destConn.Open();

                //******************************************************************************************************
                // If Complex relation and large volume data in table. Directly to use SQL Query customize it as well.
                //******************************************************************************************************

                string strQuerySting =  @"SELECT * FROM " + strTableName+" (NOLOCK) "+ strCondition;
                       
                SqlCommand sourceCommand = new SqlCommand(strQuerySting, sourceConn);

                // ensure that our destination table is empty:
               // new SqlCommand("SET IDENTITY_INSERT " + strTableName + " OFF", destConn).ExecuteNonQuery();

                DateTime start = DateTime.Now;
                //textBox1.Text ="Beginning Copy ....";
                textBox1.Invoke(new Action(() => textBox1.Text = "Beginning Copy "+ strTableName + "....\r\n"));
                // using SqlDataReader to copy the rows:
                using (SqlDataReader dr = sourceCommand.ExecuteReader())
                {
                    using (SqlBulkCopy s = new SqlBulkCopy(destConn))
                    {
                        s.DestinationTableName = strTableName;
                        s.BulkCopyTimeout = 0;
                        // s.NotifyAfter = 10000;
                        //s.SqlRowsCopied += new SqlRowsCopiedEventHandler(s_SqlRowsCopied);
                        s.WriteToServer(dr);
                        s.Close();
                    }
                }
               
                //textBox1.Text += "Copy complete in "+ DateTime.Now.Subtract(start).Seconds + "  seconds./r/n";
                textBox1.Invoke(new Action(() => textBox1.Text += "Copy complete in " + DateTime.Now.Subtract(start).Seconds + "  seconds.\r\n"));
    
            }
            catch (Exception ex)
            {
                //throw (ex);
                // String strError = ex.Message;
                MessageBox.Show(ex.ToString());
               
            }
            finally
            {
                sourceConn.Close();
                destConn.Close();
                textBox1.Invoke(new Action(() => textBox1.Text += "Finished."));
            }

        }

        private void btnRun_Click(object sender, EventArgs e)
        {
            //Sample Call method :)
            //Task.Run(() => BulkCopyYTData("DOC_CONTRACT_FILE", " WHERE CONVERT(varchar(10),chgTime ,111) BETWEEN '2021/09/01' AND '2021/12/31'"));
            Task.Run(() => BulkCopyYTData("EQU_HEADER", " WHERE delMark='N'"));
            
        }
    }
    
}