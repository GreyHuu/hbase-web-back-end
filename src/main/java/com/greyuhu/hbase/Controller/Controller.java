package com.greyuhu.hbase.Controller;

import com.greyuhu.hbase.entity.Department;
import com.greyuhu.hbase.hbase.HBaseScan;
import com.greyuhu.hbase.util.RestResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@RestController
public class Controller {
    HBaseScan hBaseScan = new HBaseScan();

    /**
     * 获取全部的部门
     *
     * @return
     * @throws IOException
     */
    @GetMapping("/get-all-dept")
    public RestResponse getAllDept() throws IOException {
        List<Department> departmentList = hBaseScan.getAllDept();
        if (departmentList != null)
            return RestResponse.succuess(departmentList);
        return RestResponse.fail("暂无数据");
    }

    /**
     * 根据rowkey查询部门
     *
     * @param params
     * @return
     * @throws IOException
     */
    @PostMapping("/get-dept-by-id")
    public RestResponse getDeptById(@RequestBody Map<String, Object> params) throws IOException {
        String rowKey = params.get("rowKey").toString();
        Department department = hBaseScan.getDeptByRowKey(rowKey);
        if (department != null)
            return RestResponse.succuess(department);
        return RestResponse.fail("没有查询到信息");
    }

    /**
     * 通过rowkey查询到其子部门
     *
     * @param params
     * @return
     * @throws IOException
     */
    @PostMapping("/get-sub-dept-by-id")
    public RestResponse getSubDeptById(@RequestBody Map<String, Object> params) throws IOException {
        String rowKey = params.get("rowKey").toString();
        List<Department> departments = hBaseScan.getAllSubDeptByRowKey(rowKey);
        if (departments != null)
            return RestResponse.succuess(departments);
        return RestResponse.fail("没有查询到信息");
    }
}
