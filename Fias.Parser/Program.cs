using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Linq.Mapping;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Xml;

namespace Fias.Parser
{
    class Program
    {
        //static List<string> Objects02 = new List<string>();

        private static void Main()
        {
            string FIAS_DIR = @"d:\Downloads\FIAS\fias_xml (10.09.2018)\";
            /*
            Thread t1 = new Thread(InsertHouses);
            Thread t2 = new Thread(InsertObjects);
            //Thread t3 = new Thread(InsertHouseInterval);
            //Thread t4 = new Thread(InsertNormativeDocument);
            Thread t5 = new Thread(InsertLandmarks);

            t1.IsBackground = true;
            t2.IsBackground = true;
            //t3.IsBackground = true;
            //t4.IsBackground = true;
            t5.IsBackground = true;

            // ToDo: Change path for your's files
            t1.Start(FIAS_DIR + @"AS_HOUSE_20160925_4f8d1d80-4422-4388-9f9a-e6f7b4caae46.XML");
            t2.Start(FIAS_DIR + @"AS_ADDROBJ_20160925_1c5ed78c-7e6d-492b-98ed-f4e70f4c1342.XML");
            //t3.Start(FIAS_DIR + @"AS_HOUSEINT_20160925_c2d9bf54-8614-4bc3-9a91-c5f60765738f.XML");
            //t4.Start(FIAS_DIR + @"AS_NORMDOC_20160925_188e315d-00c7-4764-b19f-4e5db9658971.XML");
            t5.Start(FIAS_DIR + @"AS_LANDMARK_20160925_c660f507-5464-4af8-96f2-220559212980.XML");

            do
            {
                Thread.Sleep(new TimeSpan(0, 0, 10));
            } 
            while (t1.IsAlive || t2.IsAlive);// || t3.IsAlive) || t4.IsAlive);
            */

            Console.Out.WriteLine("InsertObjects");
            InsertObjects(FIAS_DIR + @"AS_ADDROBJ_20180909_54a93a25-1dc2-4a88-94a6-01ac0cdbf848.XML");

            //Objects02.Sort();
            //Console.Out.WriteLine("Streets02.length = " + Objects02.Count);
            //Console.Out.WriteLine("Streets02[0] = " + Streets02[0]);

            Console.Out.WriteLine("InsertHouses");
            InsertHouses(FIAS_DIR + @"AS_HOUSE_20180909_aa2ef0a4-cbdf-405a-9361-1206442c837e.XML");
        }

        
        private static void InsertObjects(object o)
        {
            using (var context = new MyDataContext())
            {
                context.ExecuteCommand("TRUNCATE TABLE [dbo].[Object]");
                context.SubmitChanges();
            }

            var reader = XmlReader.Create(o.ToString());
            reader.Read();
            reader.Read();

            int counter = 0;

            using (var context = new MyDataContext())
            {
                var oList = new List<Object>();

                while (reader.Read())
                {
                    if (reader.Name == "Object")
                    {
                        var obj = new Object();
                        obj.AOLEVEL = Convert.ToInt32(reader.GetAttribute("AOLEVEL"));
                        obj.AOGUID = reader.GetAttribute("AOGUID");
                        obj.PARENTGUID = reader.GetAttribute("PARENTGUID");
                        obj.REGIONCODE = reader.GetAttribute("REGIONCODE");
                        obj.CITYCODE = reader.GetAttribute("CITYCODE");
                        obj.LIVESTATUS = Convert.ToInt16(reader.GetAttribute("LIVESTATUS"));
                        
                        if (   obj.REGIONCODE == "02"
                            // obj.CITYCODE == "014" // Стерлитамак
                            /*&& obj.AOLEVEL <= 7
                            && (
                                   obj.AOGUID == "6f2cbfd8-692a-4ee4-9b16-067210bde3fc" // Башкортостан
                                || obj.AOGUID == "450ce765-f993-4ceb-95e3-f11c6fd35778" // Стерлитамакский район
                                || obj.AOGUID == "84e0b23d-82fe-40a8-8739-55e679780dc3" // Стерлитамак город
                                || obj.AOGUID == "2b8b65ca-9421-4c25-b20f-b57dcdbb20c4" // Мариинский село

                                || obj.PARENTGUID == "84e0b23d-82fe-40a8-8739-55e679780dc3" // Стерлитамак город
                                || obj.PARENTGUID == "2b8b65ca-9421-4c25-b20f-b57dcdbb20c4" // Мариинский село
                            )*/
                            && obj.LIVESTATUS == 1)
                        {
                            obj.ACTSTATUS = Convert.ToInt32(reader.GetAttribute("ACTSTATUS"));
                            obj.AOID = reader.GetAttribute("AOID");
                            obj.AREACODE = reader.GetAttribute("AREACODE");
                            obj.AUTOCODE = reader.GetAttribute("AUTOCODE");
                            obj.ENDDATE = Convert.ToDateTime(reader.GetAttribute("ENDDATE"));
                            obj.EXTRCODE = reader.GetAttribute("EXTRCODE");
                            obj.TERRIFNSFL = reader.GetAttribute("TERRIFNSFL");
                            obj.TERRIFNSUL = reader.GetAttribute("TERRIFNSUL");
                            obj.UPDATEDATE = Convert.ToDateTime(reader.GetAttribute("UPDATEDATE"));
                            obj.IFNSFL = reader.GetAttribute("IFNSFL");
                            obj.IFNSUL = reader.GetAttribute("IFNSUL");
                            obj.OFFNAME = reader.GetAttribute("OFFNAME");
                            obj.OKATO = reader.GetAttribute("OKATO");
                            obj.OKTMO = reader.GetAttribute("OKTMO");
                            obj.OPERSTATUS = Convert.ToInt32(reader.GetAttribute("OPERSTATUS"));
                            obj.PLACECODE = reader.GetAttribute("PLACECODE");
                            obj.PLANCODE = reader.GetAttribute("PLANCODE");
                            obj.PLAINCODE = reader.GetAttribute("PLAINCODE");
                            obj.POSTALCODE = reader.GetAttribute("POSTALCODE");
                            obj.PREVID = reader.GetAttribute("PREVID");
                            obj.SEXTCODE = reader.GetAttribute("SEXTCODE");
                            obj.SHORTNAME = reader.GetAttribute("SHORTNAME");
                            obj.STARTDATE = Convert.ToDateTime(reader.GetAttribute("STARTDATE"));
                            obj.STREETCODE = reader.GetAttribute("STREETCODE");
                            obj.FORMALNAME = reader.GetAttribute("FORMALNAME");
                            obj.CENTSTATUS = Convert.ToInt32(reader.GetAttribute("CENTSTATUS"));
                            obj.CODE = reader.GetAttribute("CODE");
                            obj.CTARCODE = reader.GetAttribute("CTARCODE");
                            obj.CURRSTATUS = Convert.ToInt32(reader.GetAttribute("CURRSTATUS"));
                            obj.NEXTID = reader.GetAttribute("NEXTID");
                            obj.NORMDOC = reader.GetAttribute("NORMDOC");
                            obj.DIVTYPE = Convert.ToInt32(reader.GetAttribute("DIVTYPE"));

                            oList.Add(obj);
                            counter++;

                            /*if (obj.AOGUID == "f274f290-1a5f-457c-9cc6-7f8bad433e31")
                            {
                                Console.Out.WriteLine(obj.OFFNAME);
                                Console.Out.WriteLine(obj.AOGUID);
                            }*/

                            //if (/*obj.AOLEVEL == 7 // Street
                            //    &&*/ Objects02.BinarySearch(obj.AOGUID) < 0) 
                            //{
                                //Objects02.Add(obj.AOGUID);
                                //Objects02.Sort();
                            //}

                            if (counter % 100 == 0)
                            {
                                Console.Out.WriteLine("InsertObjects * 100");
                                context.BulkInsertAll(oList);
                                context.SubmitChanges();
                                oList.Clear();
                            }
                        }
                    }
                }

                context.BulkInsertAll(oList);
                context.SubmitChanges();
            }

            reader.Close();
            reader.Dispose();
        }

        private static void InsertHouses(object o)
        {
            using (var context = new MyDataContext())
            {
                context.ExecuteCommand("TRUNCATE TABLE [dbo].[House]");
                context.SubmitChanges();
            }

            var reader = XmlReader.Create(o.ToString());
            reader.Read();
            reader.Read();

            int counter = 0;

            using (var context = new MyDataContext())
            {
                var oList = new List<House>();

                while (reader.Read())
                {
                    if (reader.Name == "House")
                    {
                        var house = new House();
                        
                        house.AOGUID = reader.GetAttribute("AOGUID");

                        /*if (house.AOGUID == "f274f290-1a5f-457c-9cc6-7f8bad433e31")
                        {
                            Console.Out.WriteLine(house.AOGUID);
                            //Console.Out.WriteLine(house.HOUSEGUID);
                        }*/

                        //if (Objects02.BinarySearch(house.AOGUID) >= 0)
                        if (house.REGIONCODE == "02")
                        {
                            house.POSTALCODE = reader.GetAttribute("POSTALCODE");
                            house.REGIONCODE = reader.GetAttribute("REGIONCODE");
                            house.BUILDNUM = reader.GetAttribute("BUILDNUM");
                            house.COUNTER = Convert.ToInt32(reader.GetAttribute("COUNTER"));
                            house.ENDDATE = Convert.ToDateTime(reader.GetAttribute("ENDDATE"));
                            house.ESTSTATUS = Convert.ToInt32(reader.GetAttribute("ESTSTATUS"));
                            house.IFNSFL = reader.GetAttribute("IFNSFL");
                            house.IFNSUL = reader.GetAttribute("IFNSUL");
                            house.HOUSEGUID = reader.GetAttribute("HOUSEGUID");
                            house.HOUSEID = reader.GetAttribute("HOUSEID");
                            house.HOUSENUM = reader.GetAttribute("HOUSENUM");
                            house.NORMDOC = reader.GetAttribute("NORMDOC");
                            house.TERRIFNSFL = reader.GetAttribute("TERRIFNSFL");
                            house.TERRIFNSUL = reader.GetAttribute("TERRIFNSUL");
                            house.UPDATEDATE = Convert.ToDateTime(reader.GetAttribute("UPDATEDATE"));
                            house.OKATO = reader.GetAttribute("OKATO");
                            house.OKTMO = reader.GetAttribute("OKTMO");
                            house.CADNUM = reader.GetAttribute("CADNUM");
                            house.DIVTYPE = Convert.ToInt32(reader.GetAttribute("DIVTYPE"));
                            house.STARTDATE = Convert.ToDateTime(reader.GetAttribute("STARTDATE"));
                            house.STATSTATUS = Convert.ToInt32(reader.GetAttribute("STATSTATUS"));
                            house.STRUCNUM = reader.GetAttribute("STRUCNUM");

                            if (reader.GetAttribute("STRSTATUS") != null)
                            {
                                house.STRSTATUS = Convert.ToInt32(reader.GetAttribute("STRSTATUS"));
                            }

                            oList.Add(house);
                            counter++;

                            if (counter % 100 == 0)
                            {
                                System.Console.Out.WriteLine("InsertHouses * 100");
                                context.BulkInsertAll(oList);
                                context.SubmitChanges();
                                oList.Clear();
                            }
                        }
                    }
                }

                context.BulkInsertAll(oList);
                context.SubmitChanges();
            }

            reader.Close();
            reader.Dispose();
        }

        private static void InsertHouseInterval(object o)
        {
            using (var context = new MyDataContext())
            {
                context.ExecuteCommand("TRUNCATE TABLE [FIAS].[dbo].[HouseInterval]");
                context.SubmitChanges();
            }

            var reader = XmlReader.Create(o.ToString());
            reader.Read();
            reader.Read();

            int counter = 0;

            using (var context = new MyDataContext())
            {
                var oList = new List<HouseInterval>();

                while (reader.Read())
                {
                    // your code here.
                    if (reader.Name == "HouseInterval")
                    {
                        var obj = new HouseInterval();
                        obj.AOGUID = reader.GetAttribute("AOGUID");
                        obj.ENDDATE = Convert.ToDateTime(reader.GetAttribute("ENDDATE"));
                        obj.TERRIFNSFL = reader.GetAttribute("TERRIFNSFL");
                        obj.TERRIFNSUL = reader.GetAttribute("TERRIFNSUL");
                        obj.UPDATEDATE = Convert.ToDateTime(reader.GetAttribute("UPDATEDATE"));
                        obj.IFNSFL = reader.GetAttribute("IFNSFL");
                        obj.IFNSUL = reader.GetAttribute("IFNSUL");
                        obj.INTEND = Convert.ToInt32(reader.GetAttribute("INTEND"));
                        obj.INTGUID = reader.GetAttribute("INTGUID");
                        obj.INTSTART = Convert.ToInt32(reader.GetAttribute("INTSTART"));
                        obj.INTSTATUS = Convert.ToInt32(reader.GetAttribute("INTSTATUS"));
                        obj.OKATO = reader.GetAttribute("OKATO");
                        obj.OKTMO = reader.GetAttribute("OKTMO");
                        obj.POSTALCODE = reader.GetAttribute("POSTALCODE");
                        obj.STARTDATE = Convert.ToDateTime(reader.GetAttribute("STARTDATE"));
                        obj.HOUSEINTID = reader.GetAttribute("HOUSEINTID");
                        obj.COUNTER = Convert.ToInt32(reader.GetAttribute("COUNTER"));
                        obj.NORMDOC = reader.GetAttribute("NORMDOC");

                        oList.Add(obj);
                        counter++;

                        if (counter % 1000 == 0)
                        {
                            context.BulkInsertAll(oList);
                            context.SubmitChanges();
                            oList.Clear();
                        }
                    }
                }

                context.BulkInsertAll(oList);
                context.SubmitChanges();
            }

            reader.Close();
            reader.Dispose();
        }

        private static void InsertNormativeDocument(object o)
        {
            using (var context = new MyDataContext())
            {
                context.ExecuteCommand("TRUNCATE TABLE [FIAS].[dbo].[NormativeDocument]");
                context.SubmitChanges();
            }

            var reader = XmlReader.Create(o.ToString());
            reader.Read();
            reader.Read();

            int counter = 0;

            using (var context = new MyDataContext())
            {
                var oList = new List<NormativeDocument>();

                while (reader.Read())
                {
                    if (reader.Name == "NormativeDocument")
                    {
                        var obj = new NormativeDocument();

                        if (reader.GetAttribute("DOCDATE") != null)
                        {
                            obj.DOCDATE = Convert.ToDateTime(reader.GetAttribute("DOCDATE"));
                        }


                        if (reader.GetAttribute("DOCIMGID") != null)
                        {
                            obj.DOCIMGID = Convert.ToInt32(reader.GetAttribute("DOCIMGID"));
                        }

                        obj.DOCNAME = reader.GetAttribute("DOCNAME");
                        obj.DOCNUM = reader.GetAttribute("DOCNUM");


                        if (reader.GetAttribute("DOCTYPE") != null)
                        {
                            obj.DOCTYPE = Convert.ToInt32(reader.GetAttribute("DOCTYPE"));
                        }

                        obj.NORMDOCID = reader.GetAttribute("NORMDOCID");


                        oList.Add(obj);
                        counter++;

                        if (counter % 1000 == 0)
                        {
                            context.BulkInsertAll(oList);
                            context.SubmitChanges();
                            oList.Clear();
                        }
                    }
                }

                context.BulkInsertAll(oList);
                context.SubmitChanges();
            }

            reader.Close();
            reader.Dispose();
        }

        private static void InsertLandmarks(object o)
        {
            using (var context = new MyDataContext())
            {
                context.ExecuteCommand("TRUNCATE TABLE [FIAS].[dbo].[Landmark]");
                context.SubmitChanges();
            }

            var reader = XmlReader.Create(o.ToString());
            reader.Read();
            reader.Read();

            int counter = 0;

            using (var context = new MyDataContext())
            {
                var oList = new List<Landmark>();

                while (reader.Read())
                {
                    if (reader.Name == "Landmark")
                    {
                        var obj = new Landmark();
                        obj.ENDDATE = Convert.ToDateTime(reader.GetAttribute("ENDDATE"));
                        obj.TERRIFNSFL = reader.GetAttribute("TERRIFNSFL");
                        obj.TERRIFNSUL = reader.GetAttribute("TERRIFNSUL");
                        obj.UPDATEDATE = Convert.ToDateTime(reader.GetAttribute("UPDATEDATE"));
                        obj.IFNSFL = reader.GetAttribute("IFNSFL");
                        obj.IFNSUL = reader.GetAttribute("IFNSUL");
                        obj.OKATO = reader.GetAttribute("OKATO");
                        obj.OKTMO = reader.GetAttribute("OKTMO");
                        obj.POSTALCODE = reader.GetAttribute("POSTALCODE");
                        obj.AOGUID = reader.GetAttribute("AOGUID");
                        obj.STARTDATE = Convert.ToDateTime(reader.GetAttribute("STARTDATE"));
                        obj.LANDGUID = reader.GetAttribute("LANDGUID");
                        obj.LANDID = reader.GetAttribute("LANDID");
                        obj.LOCATION = reader.GetAttribute("LOCATION");
                        obj.NORMDOC = reader.GetAttribute("NORMDOC");

                        oList.Add(obj);
                        counter++;

                        if (counter % 1000 == 0)
                        {
                            context.BulkInsertAll(oList);
                            context.SubmitChanges();
                            oList.Clear();
                        }
                    }
                }

                context.BulkInsertAll(oList);
                context.SubmitChanges();
            }

            reader.Close();
            reader.Dispose();
        }

    }

    public class MyDataContext : FiasDBDataContext
    {
        public void BulkInsertAll<T>(IEnumerable<T> entities)
        {
            using (var conn = new SqlConnection(Connection.ConnectionString))
            {
                conn.Open();

                Type t = typeof (T);

                var tableAttribute = (TableAttribute) t.GetCustomAttributes(
                    typeof (TableAttribute), false).Single();
                var bulkCopy = new SqlBulkCopy(conn)
                {
                    DestinationTableName = tableAttribute.Name
                };

                var properties = t.GetProperties().Where(EventTypeFilter).ToArray();
                var table = new DataTable();

                foreach (var property in properties)
                {
                    Type propertyType = property.PropertyType;
                    if (propertyType.IsGenericType &&
                        propertyType.GetGenericTypeDefinition() == typeof (Nullable<>))
                    {
                        propertyType = Nullable.GetUnderlyingType(propertyType);
                    }

                    table.Columns.Add(new DataColumn(property.Name, propertyType));
                }

                foreach (var entity in entities)
                {
                    table.Rows.Add(
                        properties.Select(
                            property => property.GetValue(entity, null) ?? DBNull.Value
                            ).ToArray());
                }

                bulkCopy.WriteToServer(table);
            }
        }

        private bool EventTypeFilter(PropertyInfo p)
        {
            var attribute = Attribute.GetCustomAttribute(p,
                typeof (AssociationAttribute)) as AssociationAttribute;

            if (attribute == null) return true;
            if (attribute.IsForeignKey == false) return true;

            return false;
        }
    }
}
