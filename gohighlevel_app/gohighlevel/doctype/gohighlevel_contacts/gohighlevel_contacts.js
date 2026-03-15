// Copyright (c) 2026, jarlen and contributors
// For license information, please see license.txt

frappe.ui.form.on("GoHighLevel_contacts", {
    refresh(frm) {
        frm.add_custom_button(__('增量同步'), () => {
            //frappe.call(fn="ly_exp_app.utils.fhdh_tloos.get_erp_fhdh",fhdh=fhdh)
            frappe.call({
                method: "gohighlevel_app.api.data_up_task", // 后端方法路径
                /*
                args: {
                    fhdh: frm.doc.name,  // 传递当前文档数据
                    up_flag:false,
                    cc:'xxxx'
                },*/
                // 可选：开启加载动画（全局）
                freeze: true,
                freeze_message: __('全量更新数据中...'),
              
                callback:  (response)=> {
                    console.log('请求回调:',response);
                    
                    //frm.refresh();
                    console.log("查询完成，执行后续操作...");
                    //frm.reload_doc();

                    frappe.show_alert({
                        message: __(response.message.message),
                        indicator: 'success'
                    }, 5);
                },
                // 请求失败时执行（替代 try/catch）
                error: (r) => {
                  frappe.msgprint({  title: __('错误'), msg: r.exc_message || __('全量更新失败!'),
                  indicator: 'red'
                });
                }
            });
            
            //frappe.call();
            } ,
            //__("订单操作"), // 分组名
            //"btn-danger",  // 样式类
            //frappe.utils.icon("trash", "sm") // 图标
        );

    },
});
