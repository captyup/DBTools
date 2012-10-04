using System;
using System.Collections.Generic;
using System.Text;
using System.Diagnostics.CodeAnalysis;

namespace Tools
{
    public static class DateTimeTool
    {

          
        public static int GetPSCCalendarYear(DateTime date)
        {
      
            DayOfWeek FirstDayOfWeek = DayOfWeek.Monday;
            System.Globalization.CultureInfo thisCulture = System.Globalization.CultureInfo.CreateSpecificCulture("zh-tw");
            System.Globalization.Calendar calendar = thisCulture.Calendar; ;
           int week = calendar.GetWeekOfYear(date, System.Globalization.CalendarWeekRule.FirstFullWeek, FirstDayOfWeek);

            /* 
             * 決定年份 
             * 因為1月分week會是1~5，如果是52或53表示是被列為去年度的最後一周。
             */
            if (date.Month == 1 && week > 10)   // 跨年
            {
                return date.Year - 1;
            }
            else
            {
                return date.Year;
            }
        }

        public static int GetPSCCalendarWeek(DateTime date)
        {
            DayOfWeek FirstDayOfWeek = DayOfWeek.Monday;
            System.Globalization.CultureInfo thisCulture = System.Globalization.CultureInfo.CreateSpecificCulture("zh-tw");
            System.Globalization.Calendar calendar = thisCulture.Calendar; ;
            return calendar.GetWeekOfYear(date, System.Globalization.CalendarWeekRule.FirstFullWeek, FirstDayOfWeek);

        }
    }
}
