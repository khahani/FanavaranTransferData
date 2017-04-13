using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FanavaranTransformation
{
    public class Program
    {
        public static string JobFinderConnectionString
        {
            get
            {
                return System.Configuration.ConfigurationManager.AppSettings["jobFinder"].ToString().FixYEH();
            }
        }

        public static string JobCompanyConnectionString
        {
            get
            {
                return System.Configuration.ConfigurationManager.AppSettings["jobCompany"].ToString().FixYEH();
            }
        }

        public static void Main(string[] args)
        {
            int counter = 0;
            string commandText = string.Empty;

            commandText = "Select * From WorkReq";

            SqlCommand command = new SqlCommand(commandText, new SqlConnection(JobFinderConnectionString));
            command.Connection.Open();
            SqlDataReader reader = command.ExecuteReader();

            SqlConnection jobCompanyconnection = new SqlConnection(JobCompanyConnectionString);

            SqlCommand insertCommand = new SqlCommand("", jobCompanyconnection);
            insertCommand.Connection.Open();
            while (reader.Read())
            {

                string name = reader["Name"].ToString().FixYEH();
                string lastName = reader["LastName"].ToString().FixYEH();
                string shCart = reader["RegNo"].ToString().FixYEH();
                string dateSabtenam = reader["RegDate"].ToString().FixYEH();
                string fatherName = reader["FatherName"].ToString().FixYEH();
                string sNo = reader["SNo"].ToString().FixYEH();
                string bDate = reader["BDate"].ToString().FixYEH();
                string bWhere = reader["BWhere"].ToString().FixYEH();
                string relogion = reader["Relogion"].ToString().FixYEH();
                string gender = reader["Gender"].ToString().FixYEH();
                string marriage = reader["Marriage"].ToString().FixYEH();
                string nationCode = reader["NationCode"].ToString().FixYEH();
                int flagDel = 0;
                int state = 0;
                int eshteghalCart = 0;
                int eynak = 0;
                bool isDuplicate = IsDuplicate(name, lastName, sNo);

                if (isDuplicate)
                    continue;

                string insertIntoTB_InfoShakhs = string.Format("insert into TB_InfoShakhs (ShCart, dateSabtenam, Fname, Lname, FatherName, ShSh, dateBirth, Placebirth, Din, sex, TaAhol, CodeM, FlagDel, [State], EshteghalCart, Eynak)" +
                                                                            " output INSERTED.IdSh values ({0},N'{1}',N'{2}',N'{3}',N'{4}',N'{5}',N'{6}/01/01',N'{7}',N'{8}',N'{9}',N'{10}',N'{11}', {12}, {13}, {14}, {15})",
                                                                            shCart, dateSabtenam, name, lastName, fatherName, sNo, bDate, bWhere, relogion, gender, marriage, nationCode, flagDel, state, eshteghalCart, eynak);



                insertCommand.CommandText = insertIntoTB_InfoShakhs;



                long idSh = (long)insertCommand.ExecuteScalar();


                string reshte1 = reader["Branch"].ToString().FixYEH();
                string addressPosti = reader["Address"].ToString().FixYEH();
                string aya1 = "بلی";
                string bimebikari = "بلی";
                string pardakhtbime = "بلی";
                string dateStartBimeB = "1300/00/00";
                string dateEndBimeB = "1300/00/00";
                bool doneSabeghtKari = Fill_TB_SabegheKari(jobCompanyconnection, idSh, addressPosti, aya1, bimebikari, pardakhtbime, dateStartBimeB, dateEndBimeB, reshte1);

                if (!doneSabeghtKari)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("Failed TB_SabegheKari for IdSh:" + idSh.ToString().FixYEH());
                }



                string akharinMadrak = reader["Licence"].ToString().FixYEH();

                switch (akharinMadrak)
                {
                    case "":
                        akharinMadrak = "بی سواد";
                        break;
                    case "نهضت سوادآموزی":
                        akharinMadrak = "ابتدایی";
                        break;
                    case "راهنمایی":
                        akharinMadrak = "سیکل";
                        break;
                    case "دیپلم فنی":
                        akharinMadrak = "دیپلم";
                        break;
                    case "فوق دیپلم فنی":
                        akharinMadrak = "فوق دیپلم";
                        break;
                    case "لیسانس فنی":
                        akharinMadrak = "لیسانس";
                        break;
                    case "فوق لیسانس":
                    case "پزشک و بالاتر":
                        akharinMadrak = "فوق لیسانس یا بالاتر";
                        break;
                    default:
                        break;
                }

                string typeMadrak = string.Empty;
                switch (akharinMadrak)
                {
                    case "دیپلم فنی":
                    case "فوق دیپلم فنی":
                    case "لیسانس فنی":
                        typeMadrak = "فنی";
                        break;
                    default:
                        typeMadrak = "غیر فنی";
                        break;
                }

                string reshte = reader["Branch"].ToString().FixYEH();
                string saleAkhzm = "1300/00/00";
                state = 0;
                string typeUniversity = "آزاد";
                string daneshgah = reader["University"].ToString().FixYEH();
                string ShahrAkhzM = reader["City"].ToString().FixYEH();

                bool doneMadrakTahsili = Fill_TB_MadrakTahsili(jobCompanyconnection, idSh, akharinMadrak, typeMadrak, reshte, saleAkhzm, state, typeUniversity, daneshgah, ShahrAkhzM);

                if (!doneMadrakTahsili)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("Failed TB_MadrakTahsili for IdSh:" + idSh.ToString().FixYEH());
                }

                string ghavahiname = "00000000";
                bool doneMadrakAmozeshi = Fill_TB_MarakezAmozeshi(jobCompanyconnection, idSh, ghavahiname);

                if (!doneMadrakAmozeshi)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("Failed TB_MadrakAmozeshi for IdSh:" + idSh.ToString().FixYEH());
                }

                string typeShoghleAlaghe = reader["WorkType"].ToString().FixYEH();

                switch (typeShoghleAlaghe)
                {
                    case "اداري":
                        typeShoghleAlaghe = "0237";
                        break;
                    case "فني و توليدي":
                        typeShoghleAlaghe = "0000";
                        break;
                    case "ساير":
                        typeShoghleAlaghe = "0000";
                        break;
                    case "علمي تحقيقاتي":
                        typeShoghleAlaghe = "0000";
                        break;
                    case "خدماتي":
                        typeShoghleAlaghe = "0292";
                        break;
                    default:
                        typeShoghleAlaghe = "0000";
                        break;
                }

                string pishnahadKar1 = GetJobTitle(reader["JobCode1"].ToString().FixYEH());
                string pishnahadKar2 = GetJobTitle(reader["JobCode2"].ToString().FixYEH());
                string pishnahadKar3 = GetJobTitle(reader["JobCode3"].ToString().FixYEH());
                aya1 = "بلی";
                string aya2 = "بلی";
                string aya3 = "بلی";
                string aya4 = "بلی";
                string makan = "بلی";
                string tashilatModredeNiyaz = "0000";
                string alagheBeBakhshha = "0000";

                bool donePishnahadMotoghazi = Fill_TB_PishnahadMoteghazi(jobCompanyconnection, idSh, typeShoghleAlaghe, pishnahadKar1, pishnahadKar2, pishnahadKar3, aya1,
                    aya2, aya3, aya4, makan, tashilatModredeNiyaz, alagheBeBakhshha);

                if (!doneMadrakAmozeshi)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("Failed TB_MadrakAmozeshi for IdSh:" + idSh.ToString().FixYEH());
                }

                string stateKhedmat = reader["Mellitry"].ToString().FixYEH();

                string dalilMoAfiayt = reader["MellitryNote"].ToString().FixYEH();
                int razmande = 0;
                int janbaz = 0;
                int darsadJanbazi = 0;
                int azade = 0;
                int familyShahid = 0;
                int familyJanbaz = 0;
                int familyAzade = 0;
                string SBasiji = "ندارم";
                int basijAdi = 0;
                int basijFaAl = 0;
                int maloliyat = 0;


                bool doneStateNezamVazife = Fill_TB_StateNezamVazife(jobCompanyconnection, idSh, stateKhedmat, dalilMoAfiayt, razmande, janbaz, darsadJanbazi, azade, familyShahid, familyJanbaz, familyAzade,
                    SBasiji, basijAdi, basijFaAl, maloliyat);


                counter++;

                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine("Done: IdSh: " + idSh.ToString().FixYEH() + "Number: " + counter.ToString().FixYEH());

            }

            insertCommand.Connection.Close();

            reader.Close();

            command.Connection.Close();
        }

        private static bool Fill_TB_StateNezamVazife(SqlConnection connection, long idSh, string stateKhedmat, string dalilMoAfiayt, int razmande, int janbaz, int darsadJanbazi, int azade, int familyShahid, int familyJanbaz, int familyAzade, string sBasiji, int basijAdi, int basijFaAl, int maloliyat)
        {
            string commandText = @"Insert into TB_StateNezamVazife (IdSh,StateKhedmat, DalilMoAfiayt, Razmande, Janbaz, DarsadJanbazi, Azade, FamilyShahid, FamilyJanbaz, FamilyAzadeh, SBasiji, BasijiAdi, BasijiFaAl, Maloliyat)
	Values(" + idSh + ", N'" + stateKhedmat + "', N'" + dalilMoAfiayt + "', " + razmande + ", " + janbaz + ", " + darsadJanbazi + ", " + azade + ", " + familyShahid + ", " + familyJanbaz + ", " + familyAzade + ",N'" + sBasiji + "', " + basijAdi + ", " + basijFaAl + ", " + maloliyat + ")";

            SqlCommand command = new SqlCommand(commandText, connection);

            if (connection.State != System.Data.ConnectionState.Open)
                connection.Open();

            try
            {
                int result = command.ExecuteNonQuery();
                return result > 0;
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(ex.Message);
                Console.ReadKey();
                return false;
            }
        }

        private static string GetJobTitle(string jobId)
        {
            if (jobId == string.Empty || jobId == "0")
                return string.Empty;

            SqlConnection connection = new SqlConnection(JobFinderConnectionString);
            SqlCommand command = new SqlCommand(string.Empty, connection);
            command.CommandText = "Select Top(1) JobName From Job Where JobId = " + jobId;
            connection.Open();
            try
            {
                object result = command.ExecuteScalar();
                if (result != null)
                    return result.ToString().FixYEH();
                else
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("Job name not found. JobId = " + jobId.ToString().FixYEH());
                    return string.Empty;
                }
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(ex.Message);
                return string.Empty;
            }
        }

        private static bool Fill_TB_PishnahadMoteghazi(SqlConnection connection, long idSh, string typeShoghleAlaghe, string pishnahadKar1, string pishnahadKar2, string pishnahadKar3, string aya1, string aya2, string aya3, string aya4, string makan, string tashilatModredeNiyaz, string alagheBeBakhshha)
        {
            string commandText = @"Insert into TB_PishnahadMotaghazi(IdSh, TypeShogheAlaghe, PishnahadKar1, PishnahadKar2, PishnahadKar3, Aya1, Aya2, Aya3 , Aya4, TashilatMoredeNiyaz, Makan, AlagheBeBakhshha)
Values(" + idSh + ", '" + typeShoghleAlaghe + "', N'" + pishnahadKar1 + "', N'" + pishnahadKar2 + "', N'" + pishnahadKar3 + "', N'" + aya1 + "', N'" + aya2 + "', N'" + aya3 + "', N'" + aya4 + "', '" + tashilatModredeNiyaz + "', N'" + makan + "', '" + alagheBeBakhshha + "')";

            SqlCommand command = new SqlCommand(commandText, connection);

            if (connection.State != System.Data.ConnectionState.Open)
                connection.Open();

            try
            {
                int result = command.ExecuteNonQuery();
                return result > 0;
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(ex.Message);
                Console.ReadKey();
                return false;
            }
        }

        private static bool Fill_TB_MarakezAmozeshi(SqlConnection connection, long idSh, string ghavahiname)
        {
            string commandText = @"Insert into TB_MarakezAmozeshi (IdSh, Ghavahiname)
Values (" + idSh + ", N'" + ghavahiname + "')";

            SqlCommand command = new SqlCommand(commandText, connection);

            if (connection.State != System.Data.ConnectionState.Open)
                connection.Open();

            try
            {
                int result = command.ExecuteNonQuery();
                return result > 0;
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(ex.Message);
                Console.ReadKey();
                return false;
            }
        }

        private static bool Fill_TB_MadrakTahsili(SqlConnection connection, long idSh, string akharinMadrak, string typeMadrak, string reshte, string saleAkhzm, int state, string typeUniversity, string daneshgah, string shahrAkhzM)
        {
            string commandText = @"Insert into TB_MadarekTahsili (IdSh, AkharinMadrak, Daneshgah, Reshte, SaleAkhzm, [State], TypeMadrak, TypeUniver, ShahrAkhzM)
Values (" + idSh + ", N'" + akharinMadrak + "', N'" + daneshgah + "', N'" + reshte + "', N'" + saleAkhzm + "', " + state + ", N'" + typeMadrak + "', N'" + typeUniversity + "', N'" + shahrAkhzM + "')";

            SqlCommand command = new SqlCommand(commandText, connection);

            if (connection.State != System.Data.ConnectionState.Open)
                connection.Open();

            try
            {
                int result = command.ExecuteNonQuery();
                return result > 0;
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(ex.Message);
                Console.ReadKey();
                return false;
            }
        }

        private static bool Fill_TB_SabegheKari(SqlConnection connection, long idSh, string addressPosti, string aya1, string bimebikari, string pardakhtbime, string dateStartBimeB, string dateEndBimeB, string reshte1)
        {

            string commandText = @"Insert into TB_SabegheKari(IdSh, AddressPosti, BimeBikari, PardakhtBime, dateStartBimeB, DateEndBimeB, Aya1, ReShte1)
	Values (" + idSh + ", N'" + addressPosti + "', N'" + bimebikari + "', N'" + pardakhtbime + "', N'" + dateStartBimeB + "', N'" + dateEndBimeB + "', N'" + aya1 + "', N'" + reshte1 + "')";

            SqlCommand command = new SqlCommand(commandText, connection);

            if (connection.State != System.Data.ConnectionState.Open)
                connection.Open();

            try
            {
                int result = command.ExecuteNonQuery();
                return result > 0;
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(ex.Message);
                Console.ReadKey();
                return false;
            }
        }

        private static bool IsDuplicate(string name, string lastName, string sNo)
        {
            string commandText = "Select Count(*) From TB_InfoShakhs Where Fname = N'" + name + "' And Lname = N'" + lastName + "' And ShSh = N'" + sNo + "'";

            SqlCommand command = new SqlCommand(commandText, new SqlConnection(JobCompanyConnectionString));

            command.Connection.Open();

            int result = (int)command.ExecuteScalar();

            command.Connection.Close();

            return result != 0;
        }
    }

}
