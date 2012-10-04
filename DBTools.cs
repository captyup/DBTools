using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Data.Common;
using System.Reflection;
using System.Diagnostics;
using System.Text.RegularExpressions;
namespace Tools
{
    public class DBTools : IDBTools
    {

        /* sample code
              List<Block> list = new List<Block>();
                foreach (IDataRecord dr in
                    DBTools.ExecuteDataRecord(this.conn, sql))
                {
                    Block block = new Block(dr["ID"] as string);
                    block.Index = Convert.ToInt32(dr["IDX"]);
                    block.Name = dr["NAME"] as string;
                    list.Add(block);
                }
         */
        /// <summary>
        /// 傳入DbCommand 回傳 IEnumerable<IDataRecord>,支援foreach。
        /// Caller 不需要操作DbConnection的Open/Close。本方法會自動控制。
        /// </summary>
        /// <param name="cmd">已指定Connection的DbCommand。</param>
        /// <returns>IEnumerable<IDataRecord></returns>
        public IEnumerable<IDataRecord> ExecuteDataRecord(IDbCommand cmd)
        {
            IDataReader dr = ExecuteReader(cmd);
            try
            {
                while (dr.Read())
                {
                    yield return dr;
                }
            }
            finally
            {
                dr.Close();
                dr.Dispose();
            }

        }
        /// <summary>
        /// 傳入DbCommand 回傳 IEnumerable<IDataRecord>,支援foreach。
        /// Caller 不需要操作DbConnection的Open/Close。本方法會自動控制。
        /// </summary>
        /// <param name="conn">執行sql用的連線。狀態Open/Close都支援。</param>
        /// <param name="sql">要執行的sql</param>
        /// <param name="parameters">填入時請按照parameter在sql中的順序。一樣名字的只要填第一次。</param>
        /// <returns>IEnumerable<IDataRecord></returns>
        public IEnumerable<IDataRecord> ExecuteDataRecord(IDbConnection conn, string sql, params object[] parameters)
        {
            return ExecuteDataRecord(GetCommand(conn, sql, parameters));
        }
        /// <summary>
        /// 將Query Sql的結果用DataTable的方法回傳。
        /// </summary>
        /// <param name="conn">執行sql用的連線。狀態Open/Close都支援。</param>
        /// <param name="sql">要執行的sql</param>
        /// <param name="parameters">填入時請按照parameter在sql中的順序。一樣名字的只要填第一次。</param>
        /// <returns>DataTable</returns>
        public DataTable GetData(IDbConnection conn, string sql, params object[] parameters)
        {

            return GetData(GetCommand(conn, sql, parameters));

        }
        public DataTable GetData(IDbCommand cmd)
        {
            DataTable dt = new DataTable();
            dt.Locale = System.Globalization.CultureInfo.InvariantCulture;
            FillData(cmd, dt);
            return dt;
        }


        /// <summary>
        /// 依照輸入的sql，parameter values。建立DbCommand
        /// </summary>
        /// <param name="sql">要執行的sql</param>
        /// <param name="parameters">填入時請按照parameter在sql中的順序。一樣名字的只要填第一次。</param>
        /// <returns>DbCommand</returns>
        public static IDbCommand GetCommand(IDbConnection conn, string sql, object[] parameters)
        {

            IDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            FillParameters(cmd);
            if (cmd.Parameters.Count != parameters.Length)
            {
                throw new ArgumentException("cmd.Parameters.Count != parameters.Length");
            }
            for (int i = 0, count = cmd.Parameters.Count; i < count; i++)
            {
                object para = parameters[i];
                IDbDataParameter dbParameter = cmd.Parameters[i] as IDbDataParameter;
                if (para != null)
                {
                    if (para is bool)
                    {
                        if ((bool)para)
                        {
                            dbParameter.Value = bool.TrueString;
                        }
                        else
                        {
                            dbParameter.Value = bool.FalseString;
                        }
                    }
                    else if (para is DateTime)
                    {
                        dbParameter.DbType = DbType.DateTime;
                        dbParameter.Value = para;
                    }
                    else
                    {
                        dbParameter.Value = para;
                    }
                }
            }
            return cmd;
        }

        public int FillData(IDbCommand cmd, DataTable dt)
        {
            if (cmd == null)
            {
                throw new ArgumentNullException("cmd");
            }


            ConnectionState state = cmd.Connection.State;

            try
            {
                if (OnExecuting(cmd))//在執行SQL前會發出事件。如果事件處理者決定Cancel執行。就中斷執行。
                {
                    return 0;
                }

                Stopwatch sw = new Stopwatch();
                sw.Start();
                if (state == ConnectionState.Closed)
                {
                    cmd.Connection.Open();
                }
                using (IDataReader dr = cmd.ExecuteReader(state == ConnectionState.Closed ? CommandBehavior.CloseConnection : CommandBehavior.Default))
                {
                    dt.Load(dr);
                }
                int count = dt.Rows.Count;
                sw.Stop();
                OnExecuted(cmd, sw.Elapsed, count);//執行後發出事件。
                return count;
            }
            catch (Exception ex)
            {
                OnError(cmd, ex);//錯誤發生時，也發出事件。
                throw;
            }


        }
        static DbProviderFactory GetDbProviderFactoryByAssembly(Assembly assembly)
        {
            foreach (DataRow row in DbProviderFactories.GetFactoryClasses().Rows)
            {
                if (row["AssemblyQualifiedName"].ToString().IndexOf(assembly.FullName) != -1)
                {
                    return DbProviderFactories.GetFactory(row);
                }
            }



            return null;

        }
        public int ExecuteNonQuery(IDbConnection conn, string sql, params object[] parameters)
        {
            return ExecuteNonQuery(GetCommand(conn, sql, parameters));
        }
        public int ExecuteNonQuery(IDbCommand cmd)
        {
            if (cmd == null)
            {
                throw new ArgumentNullException("cmd");
            }
            if (cmd.Connection == null)
            {
                throw new InvalidOperationException("cmd.Connection == null");
            }
            ConnectionState state = cmd.Connection.State;
            try
            {
                if (OnExecuting(cmd))
                {
                    return 0;
                }
                Stopwatch sw = new Stopwatch();
                sw.Start();
                if (state == ConnectionState.Closed)
                {
                    cmd.Connection.Open();
                }
                int result = cmd.ExecuteNonQuery();
                sw.Stop();
                OnExecuted(cmd, sw.Elapsed, result);

                return result;

            }
            catch (Exception ex)
            {
                OnError(cmd, ex);
                throw;
            }
            finally
            {
                if (state == ConnectionState.Closed)
                {
                    cmd.Connection.Close();
                }
            }
        }

        /// <summary>
        /// 取得OracleDataReader，Caller可以不用handle Connection。此方法會自動開啟，並在OracleDataReader被Close時 ，自動將Connection關閉。
        /// </summary>
        /// <param name="cmd"></param>
        /// <param name="behavior"></param>
        /// <returns></returns>
        private IDataReader ExecuteReader(IDbCommand cmd)
        {
            if (cmd == null)
            {
                throw new ArgumentNullException("cmd");
            }
            if (cmd.Connection == null)
            {
                throw new InvalidOperationException("cmd.Connection == null");
            }
            try
            {
                CommandBehavior behavior = CommandBehavior.Default;
                if (cmd.Connection.State == ConnectionState.Closed)
                {
                    //如果Method進入時。Connection是關閉的，就打開。
                    cmd.Connection.Open();
                    behavior = behavior | CommandBehavior.CloseConnection;//加入CommandBehavior.CloseConnection。在DataReader被Close時同時也關閉Connection.
                }
                if (OnExecuting(cmd))
                {
                    return null;
                }
                Stopwatch sw = new Stopwatch();
                sw.Start();
                IDataReader dr = cmd.ExecuteReader(behavior);
                sw.Stop();
                OnExecuted(cmd, sw.Elapsed);
                return dr;
            }
            catch (Exception ex)
            {
                OnError(cmd, ex);
                throw;
            }
        }

        public object ExecuteScalar(IDbConnection conn, string sql, params object[] parameters)
        {
            return ExecuteScalar(GetCommand(conn, sql, parameters));
        }
        public object ExecuteScalar(IDbCommand cmd)
        {

            if (cmd == null)
            {
                throw new ArgumentNullException("cmd");
            }
            if (cmd.Connection == null)
            {
                throw new InvalidOperationException("cmd.Connection == null");
            }
            ConnectionState state = cmd.Connection.State;
            try
            {
                if (OnExecuting(cmd))
                {
                    return null;
                }
                Stopwatch sw = new Stopwatch();
                sw.Start();
                if (state == ConnectionState.Closed)
                {
                    cmd.Connection.Open();
                }
                object result = cmd.ExecuteScalar();
                sw.Stop();
                OnExecuted(cmd, sw.Elapsed, Convert.IsDBNull(result) ? 1 : 0);
                return result;
            }
            catch (Exception ex)
            {
                OnError(cmd, ex);
                throw;
            }
            finally
            {
                if (state == ConnectionState.Closed)
                {
                    cmd.Connection.Close();
                }
            }
        }
        public static void FillParameters(IDbCommand cmd)
        {
            Regex regex = new Regex(@"(?<No>('[@|:]\w+'))|[^\w](?<Yes>([:|@]\w+))");//建立Regex將符合Parameter Pattern的都找出來
            MatchCollection matches = regex.Matches(cmd.CommandText);
            foreach (Match match in matches)
            {
                if (string.IsNullOrEmpty(match.Groups["Yes"].Value))
                {
                    continue;
                }
                string paraName = match.Groups["Yes"].Value.TrimStart(':');
                if (cmd.Parameters.Contains(paraName))
                {
                    continue;
                }
                IDbDataParameter para = cmd.CreateParameter();
                para.ParameterName = paraName;
                para.Value = DBNull.Value;//預設都加入DBNull,不然para.Value=null。執行時會Error。
                cmd.Parameters.Add(para);
            }
        }
        /// <summary>
        /// 從data中找出和OracleParameter.ParameterName一樣的Property。並設定給OracleParameter.Value。
        /// </summary>
        /// <param name="cmd"></param>
        /// <param name="data"></param>
        public static void FillParametersValue(IDbCommand cmd, object data)
        {
            Type type = data.GetType();
            foreach (IDbDataParameter para in cmd.Parameters)
            {
                PropertyInfo info = type.GetProperty(para.ParameterName);//不分大小寫。
                object value = info.GetValue(data, null);
                if (value != null)
                {
                    para.Value = value;
                }
            }
        }
        /// <summary>
        /// 將OracleDataReader中找出和Object Properties同名的Column Value。
        /// </summary>
        /// <param name="dr"></param>
        /// <param name="obj"></param>
        public static void ReadValues(IDataRecord dr, object values)
        {
            //先把Column Name收集起來，並全轉成大寫。
            //目的是為了支援不分大小寫
            List<string> columns = new List<string>();
            for (int i = 0; i < dr.FieldCount; i++)
            {
                columns.Add(dr.GetName(i).ToUpper(System.Globalization.CultureInfo.InvariantCulture));
            }
            Type type = values.GetType();
            PropertyInfo[] ps = type.GetProperties();
            foreach (PropertyInfo info in ps)
            {
                //如果obj上有一個屬性是DataReader裡面沒有的就略過不處理。
                if (!columns.Contains(info.Name.ToUpper(System.Globalization.CultureInfo.InvariantCulture)))
                {
                    continue;
                }
                //取得index。注意要從columns裡面拿。而不是dr
                int index = columns.IndexOf(info.Name.ToUpper(System.Globalization.CultureInfo.InvariantCulture));
                if (!dr.IsDBNull(index))
                {
                    Type t = GetPropertyType(info);
                    //bool特別處理, 原因是OracleDB裡面沒有bool所以bool在db裡面是用char的型態存放。用Convert轉的時候會失敗。
                    if (t == typeof(bool))
                    {
                        info.SetValue(values, Convert.ToBoolean(dr[index]), BindingFlags.SetProperty, null, null, System.Globalization.CultureInfo.InvariantCulture);
                    }
                    else if (t.IsEnum)
                    {
                        info.SetValue(values, Enum.Parse(t, dr[index].ToString()), null);
                    }
                    else
                    {
                        info.SetValue(values, Convert.ChangeType(dr[index], t, System.Globalization.CultureInfo.InvariantCulture), BindingFlags.SetProperty, null, null, System.Globalization.CultureInfo.InvariantCulture);
                    }
                }
            }
        }

        private static Type GetPropertyType(PropertyInfo info)
        {
            Type t = null;
            //遇到Nullable<T>的情況要特別處理，所以把這個部分抽出來獨立做。
            if (info.PropertyType.IsGenericType && info.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                PropertyInfo valueProp = info.PropertyType.GetProperty("Value");
                t = valueProp.PropertyType;
            }
            else
            {
                t = info.PropertyType;
            }
            return t;
        }
        #region IDBTools Members

        public event EventHandler<ExecutedEventArgs> Executed;

        public event EventHandler<ExecutingEventArgs> Executing;

        public event EventHandler<ExecuteErrorEventArgs> ExecuteError;

        protected bool OnExecuting(IDbCommand cmd)
        {
            if (Executing != null)
            {
                ExecutingEventArgs args = new ExecutingEventArgs(cmd);
                Executing(this, args);
                return args.Cancel;
            }
            else
            {
                return false;
            }
        }
        protected void OnExecuted(IDbCommand cmd, TimeSpan elapsed, int count)
        {
            if (Executed != null)
            {
                ExecutedEventArgs args = new ExecutedEventArgs(cmd, elapsed, count);
                Executed(this, args);
            }
        }
        protected void OnExecuted(IDbCommand cmd, TimeSpan elapsed)
        {
            if (Executed != null)
            {
                ExecutedEventArgs args = new ExecutedEventArgs(cmd, elapsed);
                Executed(this, args);
            }
        }
        protected void OnError(IDbCommand cmd, Exception ex)
        {
            if (ExecuteError != null)
            {
                ExecuteError(this, new ExecuteErrorEventArgs(cmd, ex));
            }
        }
        #endregion
        public static DbConnection GetConnectionFromConfig(string name)
        {
            DbProviderFactory provider = DbProviderFactories.GetFactory(System.Configuration.ConfigurationManager.ConnectionStrings[name].ProviderName);
            DbConnection conn = provider.CreateConnection();
            conn.ConnectionString = System.Configuration.ConfigurationManager.ConnectionStrings[name].ConnectionString;
            return conn;
        }

    }
    public class ExecutedEventArgs : EventArgs
    {
        public ExecutedEventArgs(IDbCommand cmd, TimeSpan elapsed)
        {
            this.Command = cmd;
            this.Elapsed = elapsed;
        }
        public ExecutedEventArgs(IDbCommand cmd, TimeSpan elapsed, int count)
        {
            this.Command = cmd;
            this.Elapsed = elapsed;
            this.Count = count;
        }
        private TimeSpan _Elapsed;

        public TimeSpan Elapsed
        {
            get { return _Elapsed; }
            private set { _Elapsed = value; }
        }
        private IDbCommand _Command;

        public IDbCommand Command
        {
            get { return _Command; }
            private set { _Command = value; }
        }
        private int _Count = -1;
        /// <summary>
        /// Query結果資料筆數、NouQuery所影響的資料筆數。-1為未設定，如執行ExecuteDataRecord()時。
        /// </summary>
        public int Count
        {
            get { return _Count; }
            private set { _Count = value; }
        }


    }
    public class ExecutingEventArgs : EventArgs
    {
        public ExecutingEventArgs(IDbCommand cmd)
        {
            this.Command = cmd;
        }
        private bool _Cancel;

        public bool Cancel
        {
            get { return _Cancel; }
            set { _Cancel = value; }
        }


        private IDbCommand _Command;

        public IDbCommand Command
        {
            get { return _Command; }
            set { _Command = value; }
        }

    }
    public class ExecuteErrorEventArgs : EventArgs
    {
        public ExecuteErrorEventArgs(IDbCommand cmd, Exception ex)
        {
            this.Command = cmd;
            this.Exception = ex;
        }
        private IDbCommand _Command;

        public IDbCommand Command
        {
            get { return _Command; }
            set { _Command = value; }
        }

        private Exception _Exception;

        public Exception Exception
        {
            get { return _Exception; }
            set { _Exception = value; }
        }

    }
    public interface IDBTools
    {
        event EventHandler<ExecuteErrorEventArgs> ExecuteError;
        event EventHandler<ExecutedEventArgs> Executed;
        event EventHandler<ExecutingEventArgs> Executing;

        object ExecuteScalar(IDbConnection conn, string sql, params object[] parameters);
        object ExecuteScalar(IDbCommand cmd);


        DataTable GetData(IDbConnection conn, string sql, params object[] parameters);
        int FillData(IDbCommand cmd, DataTable dt);

        int ExecuteNonQuery(IDbCommand cmd);
        int ExecuteNonQuery(IDbConnection conn, string sql, params object[] parameters);

        IEnumerable<IDataRecord> ExecuteDataRecord(IDbConnection conn, string sql, params object[] parameters);
        IEnumerable<IDataRecord> ExecuteDataRecord(IDbCommand cmd);

    }
}