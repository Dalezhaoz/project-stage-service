using ClosedXML.Excel;
using ProjectStageService.Models;

namespace ProjectStageService.Services;

public sealed class ProjectStageExportService
{
    public byte[] Export(ProjectStageSummary summary)
    {
        using var workbook = new XLWorkbook();
        var sheet = workbook.Worksheets.Add("项目阶段汇总");

        sheet.Cell(1, 1).Value = "服务器";
        sheet.Cell(1, 2).Value = "数据库";
        sheet.Cell(1, 3).Value = "项目名称";
        sheet.Cell(1, 4).Value = "阶段名称";
        sheet.Cell(1, 5).Value = "开始时间";
        sheet.Cell(1, 6).Value = "结束时间";
        sheet.Cell(1, 7).Value = "当前状态";

        var row = 2;
        foreach (var item in summary.Records)
        {
            sheet.Cell(row, 1).Value = item.ServerName;
            sheet.Cell(row, 2).Value = item.DatabaseName;
            sheet.Cell(row, 3).Value = item.ProjectName;
            sheet.Cell(row, 4).Value = item.StageName;
            sheet.Cell(row, 5).Value = item.StartTime;
            sheet.Cell(row, 6).Value = item.EndTime;
            sheet.Cell(row, 7).Value = item.Status;
            row++;
        }

        sheet.Columns().AdjustToContents();
        using var stream = new MemoryStream();
        workbook.SaveAs(stream);
        return stream.ToArray();
    }
}
