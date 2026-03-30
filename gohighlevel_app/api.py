from highlevel import HighLevel
from highlevel.services.contacts.models import UpdateContactDto ,UpsertContactDto
import frappe,json
from deepdiff import DeepDiff
import asyncio
from gohighlevel_app.utils.gl_utils import ContactConstants,fields_map,doc_fields_map,reusable_async_loop,get_highlevel_client,get_contact_doc,get_dddress_doc,upinsert_contact_doc,get_contact_lst
from highlevel.storage import MemorySessionStorage

def get_hl_client(location_id):
    """获取 GoHighLevel 客户端实例"""
    # 替换为你实际的客户端初始化逻辑
    private_integration_token = frappe.get_value(
            "GoHighLevel_Set",          # 第1个参数：doctype
            {"check": 1, "name": location_id},               # 第2个参数：filters（字典）
            "private_integration_token" # 第3个参数：fieldname
        ) #提取私域token
    if private_integration_token:
        hl_client = HighLevel(private_integration_token=private_integration_token)
        return hl_client
    else:
        error_msg = f"未找到有效的私域token，location_id={location_id}"
        frappe.logger().error(error_msg)  # 记录错误日志
        raise ValueError(error_msg)  # 抛出异常

@frappe.whitelist(allow_guest=True)
def webhook_test():
    '''用来做webhook测试的'''
    try:
        data = frappe._dict(frappe.request.json or frappe.form_dict )
        # 1. 打印请求日志（调试必备）
        frappe.logger().error("="*50)
        frappe.logger().error("收到 GHL Webhook 请求")
        frappe.logger().error(f"事件类型: {data.get('type')}")
        frappe.logger().error(f"事件数据: {data}")

        # 2. 执行 SDK 自动处理（令牌/安装卸载/验证）
        # 修复：直接调用函数，不再使用 .on() 装饰器
        #webhook_middleware(request)

        # 3. 手动处理自定义事件（正确写法！）
        event_type = data.get('type')
        
        # =====================================
        # 在这里写所有事件的业务逻辑
        # =====================================
        if event_type == "INSTALL":
            frappe.logger().error("✅ 应用安装事件触发")
            # 你的逻辑：记录用户、初始化数据
            
        elif event_type == "UNINSTALL":
            frappe.logger().error("❌ 应用卸载事件触发")
            # 你的逻辑：清理数据
            
        elif event_type == "contact.added":
            frappe.logger().error("👤 新增客户事件触发")
            customer = data.get('data', {})
            frappe.logger().error(f"客户信息: {customer}")
            # 你的逻辑：同步CRM、发送通知
            
        elif event_type == "order.paid":
            frappe.logger().error("💰 订单支付事件触发")
            # 你的逻辑：发货、通知

        # 4. 返回成功给 GHL
        frappe.response.update({"status": "success"}) 

    except Exception as e:
        frappe.logger().error(f"处理失败: {str(e)}", exc_info=True)
        frappe.response["message"] = f"webhook处理失败: {str(e)}"
    
def gl_data_to_doc(data:dict) -> dict:
    '''将GoHighLevel的联系人数据转换为Frappe文档格式'''
    data['doctype'] = "GoHighLevel_contacts" #指定文档类型
    for k,v in data.items():
        if isinstance(v, list):
            data[k] = json.dumps(v) 
        elif isinstance(v,dict):
            data[k] = json.dumps(v) 
        elif k in ['dateUpdated','dateAdded','dateOfBirth']:
            if v:
                data[k] = v.replace('T', ' ').replace('Z', '')
                data[k] =  data[k][0: data[k].find('.')]
    
    ##gl平台数据转为frappe doc数据
    data =  {fields_map.get(k,k):v  for k,v in data.items()  }
    return data

def doc_to_dict(doc_data):
    '''将Frappe文档数据转换为字典格式，便于对比和处理'''
    doc_key = ('name','owner','creation','modified','modified_by','docstatus','idx','parent','parenttype','parentfield','_user_tags','_comments','dateUpdated','dateAdded')

    return {  k: v for k, v in doc_data.items() if k not in doc_key }

def upsert_contact(location_id,contacts_id):
    '''处理GoHighLevel的webhook事件，更新/创建事件'''
    try:
        #提取账号信息
        hl_client = get_hl_client(location_id)
        doc_flag = frappe.db.exists("GoHighLevel_contacts", contacts_id) #确认联系人是否存在
        #处理联系人创建事件
        gh_doc = asyncio.run(hl_client.contacts.get_contact(contact_id=contacts_id))  #提取gl记录
        gh_doc = gh_doc.get('contact',{})
        gh_data = gl_data_to_doc(gh_doc)  # 转换数据格式，适配Frappe文档字段要求
        
        res = {
            "event_type": "更新" if doc_flag else "创建",
            "docname": contacts_id,
            "location_id": location_id,
            "raw_data": gh_data
        }

        if doc_flag:
            doc = frappe.get_doc("GoHighLevel_contacts",contacts_id) #提取文档
            old_doc = doc_to_dict(doc.as_dict()) #转换为字典格式，便于后续对比
            doc.update(gh_doc)
            doc.save(ignore_permissions=True) #保存文档
            new_doc = doc_to_dict(doc.as_dict()) #转换为字典格式，便于后续对比
            diff = DeepDiff(old_doc, new_doc) #对比更新前后文档差异
            res['diff'] = diff #将差异结果添加到返回

            if len(diff) == 0:
                res['event_type'] = '无变化' #如果没有差异，说明数据未发生变化

        else:
            doc = frappe.new_doc("GoHighLevel_contacts") #创建文档
            doc.update(gh_doc)
            doc.insert(ignore_permissions=True) #插入文档

        res['data'] = doc.as_dict() #将文档数据添加到返回
        frappe.db.commit() #提交数据库事务

        frappe.logger().error(f"处理GoHighLevel webhook事件结果:\n {res} ") #记录处理结果日志
        frappe.response["message"] = f"成功处理GoHighLevel webhook事件: {res['event_type']} 联系人 {res['docname']}" #
        frappe.response["res"] = res #返回处理结果给前端

    except Exception as e:
        error_msg = f"处理GoHighLevel webhook 更新插入事件时出错：{str(e)}"
        frappe.logger().error(f"{error_msg}, GoHighLevel webhook 更新插入事件 处理错误")  # 记录错误日志
        frappe.throw(error_msg)  # 抛出异常

@frappe.whitelist(allow_guest=True)
def webhook_func():
    """ 处理GoHighLevel的webhook事件(创建/删除,修改)，根据事件类型和请求方法进行相应的操作,webhook 网关"""
    args = frappe._dict(frappe.request.json or frappe.form_dict )
    
    contacts_id = args.contact_id #提取 contacts 的 id 等价于 name
    event_type = args.workflow.get('name') #提取事件类型

    location_id = args.location.get('id') #提取位置id(绑定账号相关信息)
    method=frappe.request.method #请求方法
    frappe.logger().error(f"记录webhook 请求响应:\n {args}")
    try:

        if (event_type == 'GoHighLevel to Frappe Update' or event_type ==  'GoHighLevel to Frappe Lead Created') and method in ['POST','PUT']:
            upsert_contact(location_id,contacts_id)
            
        if event_type == 'GoHighLevel to Frappe Delete' and method in ['POST','DELETE']:
            #处理联系人创建事件
            res = {
                "event_type": "删除",
                "docname": contacts_id,
                "location_id": location_id,
            }   
            if frappe.db.exists("GoHighLevel_contacts", contacts_id) : #确认联系人是否存在
                doc = frappe.get_doc("GoHighLevel_contacts",contacts_id) #提取文档
                doc.delete(ignore_permissions=True) #删除文档
                frappe.db.commit() #提交数据库事务
                res['status'] = '成功'
            else:
                res['status'] = f'失败:文档不存在{contacts_id}'

            frappe.logger().error(f"处理GoHighLevel webhook事件结果:\n {res} ") #记录处理结果日志
            frappe.response["message"] = f"成功处理GoHighLevel webhook事件: {res['event_type']} 联系人 {res['docname']}" #
            frappe.response["res"] = res #返回处理结果给前端

    except Exception as e:
        error_msg = f"处理GoHighLevel webhook事件时出错：{str(e)}"
        frappe.logger().error(f"{error_msg}, GoHighLevel webhook处理错误")  # 记录错误日志
        frappe.throw(error_msg)  # 抛出异常

@frappe.whitelist(allow_guest=True)
def webhook_func_2():
    """ 处理GoHighLevel的webhook事件(创建/删除,修改)，根据事件类型和请求方法进行相应的操作,webhook 网关 V2版本"""
    #提取参数
    args = frappe._dict(frappe.request.json or frappe.form_dict )
    
    
    try:
        #获得联系人id
        contacts_id = args.contact_id #提取 contacts 的 id 等价于 name
        #获取事件类型
        event_type = args.workflow.get('name') #提取事件类型
        location_id = args.location.get('id') #提取位置id(绑定账号相关信息)
        method=frappe.request.method #请求方法
        frappe.logger().error(f"记录webhook 请求响应:\n {args}")
        ghc = get_highlevel_client(location_id) #获取gh客户端实例
        gh_doc = asyncio.run(ghc.contacts.get_contact(contact_id=contacts_id))  #提取gl记录
        gh_doc = gh_doc.get('contact',{})

        if (event_type == 'GoHighLevel to Frappe Update' or event_type ==  'GoHighLevel to Frappe Lead Created') and method in ['POST','PUT']:
            res = upinsert_contact_doc(gh_doc)
        else:
            pass

        frappe.db.commit() #提交数据库事务

        frappe.logger().error(f"处理GoHighLevel webhook事件结果:\n {res.as_dict()} ") #记录处理结果日志
        frappe.response["message"] = f"成功处理GoHighLevel webhook事件: 联系人 {res.name}" #
        frappe.response["res"] = res #返回处理结果给前端

    except Exception as e:
        error_msg = f"处理GoHighLevel webhook事件时出错：{str(e)}"
        frappe.logger().error(f"{error_msg}, GoHighLevel webhook处理错误")  # 记录错误日志
        frappe.throw(error_msg)  # 抛出异常  
        frappe.response["message"] =error_msg




@frappe.whitelist(allow_guest=True)
def data_up_task():
    '''定时任务：同步GoHighLevel联系人数据到Frappe'''
    try:
        #获取所有启用状态的记录,收集name,私域token
        gh_lst =  frappe.get_all('GoHighLevel_Set',fields=['name', 'locationid','private_integration_token'] ,filters={'check': 1})

        async def get_contact_lst():
            '''获取联系人列表的异步函数'''
            #frappe.msgprint(f"token list: {gh_lst}", indicator="red")
            all_contacts = []  # 存储所有记录的联系人数据
                
            for ght in gh_lst:
                # 跳过空token，避免初始化失败
                pit = ght.get('private_integration_token')
                location_id = ght.get('locationid')
                if not pit or not location_id:
                    frappe.logger().error(
                        message=f"记录 {ght.get('name')} 缺少token或locationid",
                        title="GoHighLevel数据同步-参数缺失"
                    )
                    continue
                
                # 初始化gh客户端
                ghc = HighLevel(private_integration_token=pit)
                # 第一步：获取总条数
                contacts_meta = await ghc.contacts.get_contacts(
                    location_id=location_id,
                    limit=1  # 仅获取meta信息
                )
                total = contacts_meta.get("meta", {}).get('total', 0)
                if total == 0:
                    frappe.msgprint(f"记录 {ght.get('name')} 无联系人数据")
                    return []
                    
                # 第二步：获取全量联系人（注意：limit有上限，超大数量需分页）
                contacts_data = await ghc.contacts.get_contacts(location_id=location_id,limit=total)
                
                contacts = contacts_data.get('contacts', [])
                all_contacts.extend(contacts)
            return all_contacts 
       
        # 第一步：获取总条数
        contacts = asyncio.run( get_contact_lst())    
        #print(f"记录 {ght.get('name')} 同步到 {len(contacts)} 条联系人")

        res = {
            '账号数量':len(gh_lst),
            'countacts' :len(contacts),
            'data':{}
        }

        for d in contacts:
            #d['doctype'] = "GoHighLevel_contacts"
            #d['name'] = f"{d['locationId']}-{d['id']}"
            #d['name'] = d['id']

            d = gl_data_to_doc(d)  # 转换数据格式，适配Frappe文档字段要求

            frappe.logger().error(f"xxxx数据:{d}")  # 记录错误日志
            #确认联系人是否存在
            if frappe.db.exists(d['doctype'], d['id']):
                #存在则更新(进行差异更新)
                doc = frappe.get_doc(d['doctype'], d['id'])
                if doc:
                    old_doc = doc_to_dict(doc.as_dict()) #转换为字典格式，便于后续对比
                    doc.update(d)
                    doc.save(ignore_permissions=True)
                    new_doc = doc_to_dict(doc.as_dict()) 
                    diff = DeepDiff(old_doc, new_doc)
                    frappe.logger().error(f"new_doc数据:{new_doc}")  # 记录错误日志


                    # 5. 返回结果
                    res['data'][doc.name] =  {
                        "status": "updated" if len(diff) >0 else 'pass',
                        "docname": doc.name,
                        #"diff_result": diff_result,
                        "update_data": diff  
                    }

            
            else:
                #不存在,插入
                #frappe.logger().error(f"插入数据:{d}")  # 记录错误日志
                doc = frappe.new_doc(d['doctype'])
                doc.update(d)
                doc.insert(ignore_permissions=True)
                res['data'][doc.name] ={
                    "status": "inserted",
                    "docname": doc.name,
                    "inserted_data":doc.as_dict()
                }
                frappe.logger().error(f"new_doc数据:{doc.as_dict()}")  # 记录错误日志

            
        frappe.db.commit()    
        return res  # 可选：返回数据给前端

    except Exception as e:
        frappe.db.rollback()  # 异常回滚, 不涉及数据写入,可注释
        # 更友好的异常提示，便于排查
        error_msg = f"处理GoHighLevel数据时出错：{str(e)}"
        frappe.logger().error(f"{error_msg}, GoHighLevel数据同步错误")  # 记录错误日志
        frappe.throw(error_msg)  # 抛出异常


@frappe.whitelist(allow_guest=True)
def data_up_task_2():
    '''定时任务：同步GoHighLevel联系人数据到Frappe'''
    try:
        out = []
        gh_lst =  frappe.get_all('GoHighLevel_Set',fields=['name', 'locationid','private_integration_token'] ,filters={'check': 1})
        for ght in gh_lst:
            pit = ght.get('private_integration_token')
            location_id = ght.get('locationid')
            if not pit or not location_id:
                frappe.logger().error(
                    message=f"记录 {ght.get('name')} 缺少token或locationid",
                    title="GoHighLevel数据同步-参数缺失"
                )
                continue

            contacts = get_contact_lst(location_id)
            if contacts:
                frappe.msgprint(f"记录 {len(contacts)}条联系人数据")
                for contact in contacts:
                    doc = upinsert_contact_doc(contact)
                    out.append(doc.as_dict())
        return out
        
    except Exception as e:
        frappe.db.rollback()  # 异常回滚, 不涉及数据写入,可注释
        # 更友好的异常提示，便于排查
        error_msg = f"处理GoHighLevel数据时出错：{str(e)}"
        frappe.logger().error(f"{error_msg}, GoHighLevel数据同步错误")  # 记录错误日志
        frappe.throw(error_msg)  # 抛出异常
