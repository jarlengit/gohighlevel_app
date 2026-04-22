from highlevel import HighLevel
from highlevel.services.contacts.models import UpdateContactDto ,UpsertContactDto,CreateContactDto
import frappe,json
from deepdiff import DeepDiff
import asyncio,requests
from gohighlevel_app.utils.gl_utils import ContactConstants,fields_map,doc_fields_map,reusable_async_loop,get_highlevel_client,get_contact_doc,get_address_doc,upinsert_contact_doc,get_contact_lst
from highlevel.storage import MemorySessionStorage
from frappe.model.document import Document
import traceback

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

def del_contact(contacts_id):
    '''删除联系人事件'''
    try:
        
        #确认联系人是否存在
        if doc_name:= frappe.db.exists("Contact", { 'custom_gohighlevel_contact_id':contacts_id } ):
            doc = frappe.get_doc("Contact",doc_name) #提取文档
            doc.delete(ignore_permissions=True) #删除文档
            frappe.db.commit() #提交数据库事务
            return True
        else:
            return False
    except Exception as e:
        error_msg = f"处理GoHighLevel webhook 删除事件时出错：{str(e)}"
        frappe.logger().error(f"{error_msg}, GoHighLevel webhook 删除事件 处理错误")  # 记录错误日志
        frappe.throw(error_msg)  # 抛出异常

@frappe.whitelist(allow_guest=True)
def webhook_func():
    """ 处理GoHighLevel的webhook事件(创建/删除,修改)，根据事件类型和请求方法进行相应的操作,webhook 网关"""

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
        event_type = data.type

        contacts_id = data.id #提取 contacts 的 id 等价于 name
        location_id = data.locationId #提取位置id(绑定账号相关信息)
        #method=frappe.request.method #请求方法
        
        doc = None #定义doc变量，便于后续返回结果使用"

        # =====================================
        # 在这里写所有事件的业务逻辑
        # =====================================
        if event_type == "INSTALL":
            '''
            {   
                #基础事件标识（固定字段）
                "type": "INSTALL",                      #事件类型：固定为 INSTALL 表示「应用安装」
                "appId": "69c53778c480e55ef298a2fe",    #应用唯一ID：平台中该应用的全局唯一标识
                "versionId": "69c53778c480e55ef298a2fe" #应用版本ID：当前安装的应用版本唯一标识
                
                #安装核心上下文（安装到哪里 / 谁安装）
                "installType": "Location",                      #安装类型：Location = 安装到「指定位置/站点/实例」,代表应用按位置 / 站点隔离安装（多租户架构，不同位置数据独立）
                "locationId": "7m9DrwbOTNKIjE7ZZBpa",           # 位置ID：安装目标位置的唯一标识（如企业站点、部门、工作区）
                "companyId": "etyWGtgtHqgx1WQqXS9l",            #企业ID：安装应用的企业主体唯一标识
                "userId": "XqPe4GX2Comd3QnQw32G",               #用户ID：执行安装操作的管理员/用户唯一标识

                # 企业主体信息
                "companyName": "Growth App",           # 企业名称：安装应用的公司名称
                "isWhitelabelCompany": true,           # 是否白标企业：标识安装应用的企业是否为白标客户（true/false）
                
                #白标配置详情（自定义品牌信息）SaaS 核心功能：企业隐藏平台原生品牌，使用自定义品牌、域名、Logo独立运营（贴牌私有化）。

                "whitelabelDetails": {
                    "logoUrl": "https://msgsndr-private.storage.googleapis.com/companyPhotos/c04059bb-50ff-44f4-9b04-27ffb59652e0.png", #企业Logo：安装应用的企业自定义Logo图片URL
                    "domain": "crm.getgrowth.systems"                                                                                   #企业自定义独立域名（白标专属访问域名）
                },
                #辅助信息
                "trial": {},    #试用信息：空对象 = 未开启试用/正式版安装
                "timestamp": "2026-03-30T05:57:33.283Z",    #试用信息：空对象 = 未开启试用/正式版安装
                "webhookId": "b606c0c6-f89d-4be7-9068-68e01bfcbbb8" #webhook流水号(唯一不重复)：每次事件请求都会携带一个全局唯一的 webhookId，便于排查日志、追踪事件处理流程

                身份隔离：通过 companyId + locationId 实现多租户隔离，保证不同企业 / 位置的数据独立
                白标优先：isWhitelabelCompany=true，开发者必须加载企业自定义 Logo 和域名，不能使用平台原生品牌
                事件唯一性：webhookId 用于防止重复处理回调（幂等性校验）
                安装场景：企业管理员在 SaaS 平台，为指定站点（Location）安装了应用
                存储数据：保存 appId/companyId/locationId 作为租户唯一标识
                初始化租户：根据企业 ID、位置 ID 创建独立数据空间
                白标配置：加载自定义 Logo 和域名，渲染企业专属界面
                幂等校验：用 webhookId 避免重复安装初始化
                权限配置：为 userId 分配应用管理员权限
            }
            
            '''
            doc_name = frappe.db.exists("GoHighLevel_Set", { 'locationid':location_id } ) #确认记录是否存在
            if doc_name is None:
                doc = frappe.new_doc("GoHighLevel_Set") #创建文档
                doc.locationId = location_id
                doc.位置名称 = f"{data.get('companyId')}_data.get('locationId')"
                #doc.private_integration_token = '' #安装时先不设置token，等用户配置后再更新
                doc.insert(ignore_permissions=True) #插入文档
                frappe.db.commit() #提交数据库事务
            frappe.logger().error("✅ 应用安装事件触发")
            # 你的逻辑：记录用户、初始化数据
            
        elif event_type == "UNINSTALL":
            '''
            {
            "type": "UNINSTALL",                                #事件类型
            "appId": "69c53778c480e55ef298a2fe",                #appID
            "versionId": "69c53778c480e55ef298a2fe",            #版本id
            "locationId": "7m9DrwbOTNKIjE7ZZBpa",               #位置id（绑定账号相关信息）
            "timestamp": "2026-03-30T05:57:15.950Z",            #事件发生时间
            "webhookId": "3cfef536-da75-4a1d-97fa-5ba20bc41fe7" #webhook流水号(唯一不重复)
            }
            
            '''
            frappe.logger().error("❌ 应用卸载事件触发")
            # 你的逻辑：清理数据
            '''            
            doc = frappe.get_doc("GoHighLevel_Set", location_id)
            doc.delete(ignore_permissions=True) #删除文档
            frappe.db.commit() #提交数据库事务
            '''
        
        ghc =  get_hl_client(location_id)

        if event_type == "ContactDelete":
            '''
            {
                "type": "ContactDelete",
                "locationId": "7m9DrwbOTNKIjE7ZZBpa",
                "versionId": "69c53778c480e55ef298a2fe",
                "appId": "69c53778c480e55ef298a2fe",
                "id": "F6WiUxSG2wvj84LzFkXG",
                "firstName": "webhook....",
                "lastName": "test",
                "email": "303217473@qq.com",
                "phone": "+8615050559924",
                "dndSettings": {
                    "Email": {
                    "code": "103",
                    "message": "Updated by 'DevOps Zhang' at 2026-03-26T13:15:09.433Z",
                    "status": "active"
                    }
                },
                "tags": [],
                "country": "CA",
                "dateAdded": "2026-03-26T13:15:09.387Z",
                "customFields": [],
                "attributionSource": {
                    "medium": "manual",
                    "mediumId": null,
                    "sessionSource": "CRM UI"
                },
                "timezone": "Etc/GMT+12",
                "timestamp": "2026-03-29T13:46:44.227Z",
                "webhookId": "8ba8264c-42b2-46ac-a8ba-041da618ccf7"
            }
            '''
            del_contact(contacts_id)
            frappe.logger().error("👤 联系人删除")
        
            
        elif event_type == "ContactCreate":
            '''
            {
                "type": "ContactCreate",
                "locationId": "7m9DrwbOTNKIjE7ZZBpa",
                "versionId": "69c53778c480e55ef298a2fe",
                "appId": "69c53778c480e55ef298a2fe",
                "id": "iTvqHycQB2FOGVmnA5KO",
                "firstName": "webhook创建",
                "lastName": "xxxxx",
                "email": "303217473@qq.com",
                "phone": "+8615050559924",
                "tags": [],
                "country": "CA",
                "dateAdded": "2026-03-29T13:52:03.538Z",
                "customFields": [],
                "attributionSource": {
                    "medium": "manual",
                    "mediumId": null,
                    "sessionSource": "CRM UI"
                },
                "timestamp": "2026-03-29T13:52:04.212Z",
                "webhookId": "dfffb990-4b0a-4411-8ba4-995e3f0f313f"
            }
            '''
            #upsert_contact(location_id,contacts_id)
            gh_doc = asyncio.run(ghc.contacts.get_contact(contact_id=data.id))  #提取gl记录
            gh_doc = gh_doc.get('contact',{})
            doc =  upinsert_contact_doc(gh_doc)
            frappe.logger().error("👤 新增客户事件触发")

            # 你的逻辑：同步CRM、发送通知
            
        elif event_type == "ContactUpdate":
            #联系人更新
            frappe.logger().error("💰 联系人更新")
            # 你的逻辑：发货、通知
            #upsert_contact(location_id,contacts_id)
            gh_doc = asyncio.run(ghc.contacts.get_contact(contact_id=data.id))  #提取gl记录
            gh_doc = gh_doc.get('contact',{})
            frappe.logger().error(f"👤 更新联系人数据{gh_doc}")

            doc =  upinsert_contact_doc(gh_doc)
            frappe.logger().error(f"👤 更新联系人{doc.as_dict()}")

            #gh_doc = gh_doc.get('contact',{})

        # 4. 返回成功给 GHL
        frappe.response.update({"status": "success",'doc': doc.as_dict() if doc else {} }) 



    except Exception as e:
        frappe.logger().error(f"处理失败: {str(e)}", exc_info=True)
        frappe.response["message"] = f"webhook处理失败: {str(e)}"
    
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

        error_msg = f"处理GoHighLevel数据时出错：{str(e)},{traceback.format_exc()}"
        frappe.logger().error(f"{error_msg}, GoHighLevel数据同步错误")  # 记录错误日志
        frappe.throw(error_msg)  # 抛出异常


def __data_up_task():
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
            '''
            contacts = frappe.enqueue(
                get_contact_lst,
                # 直接传递参数，不要包 kwargs
                location_id=location_id,
                queue="long"

            )'''
            if contacts:
                frappe.msgprint(f"记录 {len(contacts)}条联系人数据")
                for i,contact in enumerate (contacts):
                    contact['custom_custom_gohighlevel_locationid'] = location_id
                    doc = upinsert_contact_doc(contact)
                    out.append(doc.as_dict())
                    frappe.logger().error(f"{i}, GoHighLevel数据同步{doc.name}")  # 记录错误日志

        return out
        
    except Exception as e:
        frappe.db.rollback()  # 异常回滚, 不涉及数据写入,可注释
        # 更友好的异常提示，便于排查
        error_msg = f"处理GoHighLevel数据时出错：{str(e)},{traceback.format_exc()}"
        frappe.logger().error(f"{error_msg}, GoHighLevel数据同步错误")  # 记录错误日志
        frappe.throw(error_msg)  # 抛出异常


@frappe.whitelist(allow_guest=True)
def data_up_task_2():
    '''定时任务：同步GoHighLevel联系人数据到Frappe'''
    res = frappe.enqueue(
        __data_up_task,
        queue="long"
    )
    return {
        "status": "queued",
        "job_id": res.id  # 只返回 ID
    }


    
""" 
# HighLevel 开发者后台获取
GHL_CLIENT_ID="69c53778c480e55ef298a2fe-mne5sst3"
GHL_CLIENT_SECRET="d95ddd33-c8d3-409e-a86f-898fc58c7324"
# 重定向URI：必须和GHL后台配置的完全一致（ngrok生成的HTTPS地址）
GHL_REDIRECT_URI= "https://damion-perigean-aria.ngrok-free.dev/oauth/callback"
# GHL OAuth固定端点（无需修改）
GHL_AUTH_URL= "https://marketplace.gohighlevel.com/oauth/chooselocation"
GHL_TOKEN_URL="https://services.leadconnectorhq.com/oauth/token"

@frappe.whitelist(allow_guest=True)
def oauth_callback():
    # 1. 获取 GHL 传回的授权码（Frappe 方式获取 GET 参数）
    auth_code = frappe.request.args.get("code")
    
    if not auth_code:
        frappe.local.response["http_status_code"] = 400
        return {"error": "未获取到授权码"}

    # 2. 构造兑换令牌参数
    token_params = {
        "client_id": GHL_CLIENT_ID,
        "client_secret": GHL_CLIENT_SECRET,
        "redirect_uri": GHL_REDIRECT_URI,
        "code": auth_code,
        "grant_type": "authorization_code"
    }

    try:
        # 3. 请求 HighLevel 令牌接口
        with requests.post(GHL_TOKEN_URL, data=token_params) as response:
            token_data = response.json()

            # 4. 成功返回
            if response.status_code == 200:
                return {
                    "message": "HighLevel App 认证成功！",
                    "access_token": token_data.get("access_token"),
                    "refresh_token": token_data.get("refresh_token"),
                    "expires_in": token_data.get("expires_in")
                }
            else:
                frappe.local.response["http_status_code"] = 400
                return {"error": "令牌兑换失败", "details": token_data}

    except Exception as e:
        frappe.local.response["http_status_code"] = 500
        return {"error": "请求异常", "details": str(e)}
"""


"""
{
"owner": "sj@ly.com",
"creation": "2026-03-21 11:56:29.510515",
"modified": "2026-03-24 22:06:44.664445",
"modified_by": "Guest",
"docstatus": 0,
"idx": 0,
"sync_with_google_contacts": 0,
"doctype": "Contact",
#是否主要联系人
"is_primary_contact": 0,

一下为有效数据

    "name": "xdddooooo vvvvvvvv",
    
    "first_name": "xdddooooo",
    "middle_name": "xdddooooo vvvvvvvv",
    "last_name": "vvvvvvvv",
    "full_name": "xdddooooo xdddooooo vvvvvvvv vvvvvvvv",

    "email_id": "3032174cccczzzz73@qq.com",
    "address": "cccccccccccc-Billing-2",

    "status": "Passive",
    "phone": "",
    "mobile_no": "",
    "custom_gohighlevel_contact_id": "w7NeTx4NxTfHrdxUB9Nu",
    "image": "",
    "pulled_from_google_contacts": 0,
  
    "unsubscribed": 0,
    
    "links": [],
    "phone_nos": [],
    "email_ids": [
        {
            "name": "24e1alqr0e",
            "owner": "Guest",
            "creation": "2026-03-21 11:56:29.510515",
            "modified": "2026-03-24 22:06:44.664445",
            "modified_by": "Guest",
            "docstatus": 0,
            "idx": 1,
            "email_id": "3032174cccczzzz73@qq.com",
            "is_primary": 1,
            "parent": "xdddooooo vvvvvvvv",
            "parentfield": "email_ids",
            "parenttype": "Contact",
            "doctype": "Contact Email"
        }
    ]
}

转换为 gohighlevel 联系人数据格式：

firstName: Optional[str] = doc.first_name
lastName: Optional[str] = doc.last_name #
name: Optional[str] = doc.full_name #全名
email: Optional[str] = doc.email_id #email
phone: Optional[str] = doc.phone    #手机
address1: Optional[str] = doc.address #地址
city: Optional[str] = None      #城市
state: Optional[str] = None     #省/洲
postalCode: Optional[str] =  #邮政编码
website: Optional[str] = None   #网站
timezone: Optional[str] = None  #时区
source: Optional[str] = None    #来源
country: Optional[str] = None   #国家代码
assignedTo: Optional[str] = None

"""


def on_gohighlevel_contacts_before_insert(doc: Document, method=None):
    """绑定glh 联系人  创建事件"""
    # 示例：自动计算总金额
    try:
        #ghl_id = doc.custom_gohighlevel_contact_id
        ghl_locationid =doc.custom_custom_gohighlevel_locationid
        #if ghl_locationid:
        if not ghl_locationid:
            return None

        # 获取GHL客户端
        ghc = get_hl_client(ghl_locationid)
        
        out = {
            'firstName':    doc.first_name,
            'lastName':     doc.last_name ,#
            'name':         doc.full_name, #全名
            'email':        doc.email_id, #email
            'phone':        doc.phone,    #手机
        }

        if doc.address :
            addr_doc = frappe.get_doc('Address',doc.address)
            out['address1'] = doc.address #地址
            out['city'] = addr_doc.city         #城市
            out['state'] = addr_doc.state       #省洲
            out['postalCode'] = addr_doc.pincode #邮政编码
        
            if addr_doc.country:
                country_doc = frappe.get_doc('Country',addr_doc.country)

            if country_doc:
                out['country'] = country_doc.code.upper() # #国家代码


        out = {k:v for k,v in out.items() if v is not None}

        out['locationId'] = ghl_locationid

        create_date = CreateContactDto(**out)

        with reusable_async_loop() as loop:
            _t = loop.run_until_complete(ghc.contacts.create_contact(create_date.model_dump(exclude_none=True)))
            out['_t'] = _t.get("contact", {})
            frappe.db.set_value("Contact",doc.name,'custom_gohighlevel_contact_id',_t.get("contact", {}).get('id'))
            frappe.db.commit() #提交数据库事务
            #doc.custom_gohighlevel_contact_id = _t.get("contact", {}).get('id')
            #_t =ghc.contacts.delete_contact(ghl_id)
            #frappe.logger().error(f"{ghl_id,} 账户更新成功!\n{doc.as_dict()}\n{_t}")  # 记录错误日志
            doc_dict = doc.as_dict()
            out['doc'] = doc_dict
            log_message = f"{_t.get('contact', {}).get('id')} 账户插入成功!\n{doc_dict}\n{_t}"
            frappe.logger().error(log_message)

        return out

    except Exception as e:
        error_msg = f"创建 GoHighLevel账户 {doc.name}时出错：{str(e)},{traceback.format_exc()}"
        frappe.logger().error(f"{error_msg}")  # 记录错误日志
        #frappe.throw(error_msg)  # 抛出异常
   

def on_gohighlevel_contacts_update(doc: Document, method=None):
    """'''绑定glh 联系人  更新事件'''"""
    # 示例：校验客户信用额度
    try:
        ghl_id = doc.custom_gohighlevel_contact_id
        ghl_locationid =doc.custom_custom_gohighlevel_locationid
        #if ghl_locationid:
        # 没有GHL信息直接退出，绝不报错
        #if not ghl_id:
        #    pass
       
        
        if not ghl_locationid:
            frappe.throw(f"ghl客户端初始化失败无法更新联系人!!!")

        # 获取GHL客户端
        ghc = get_hl_client(ghl_locationid)
        
        #提取数据
        
        out = {
            'firstName':    doc.first_name,
            'lastName':     doc.last_name ,#
            'name':         doc.full_name, #全名
            'email':        doc.email_id, #email
            'phone':        doc.phone,    #手机
        }
        #提取地址信息
        if doc.address :
            addr_doc = frappe.get_doc('Address',doc.address)

            out['address1'] = doc.address #地址
            out['city'] = addr_doc.city         #城市
            out['state'] = addr_doc.state       #省洲
            out['postalCode'] = addr_doc.pincode #邮政编码

            #提取国家代码
            if addr_doc.country:
                out['country'] = frappe.get_value('Country',addr_doc.country,'code')
                if out.get('country'):
                    out['country'] = out['country'].upper() #国家代码转大写
                
        #字段过滤空值或空字符串
        out = {k:v for k,v in out.items() if v is not None or v != ''}
        #位置代码
        out['locationId'] = ghl_locationid
        
        #生成更新载体
        update = UpdateContactDto(**out)

        with reusable_async_loop() as loop:
            _t = loop.run_until_complete(ghc.contacts.update_contact(ghl_id,update.model_dump(exclude_none=True)))
            

                #_t =ghc.contacts.delete_contact(ghl_id)
            #frappe.logger().error(f"{ghl_id,} 账户更新成功!\n{doc.as_dict()}\n{_t}")  # 记录错误日志
            doc_dict = doc.as_dict()
            out['doc'] = doc_dict
            success_msg = f"联系人 {ghl_id} 已同步更新到GoHighLevel（操作人：{frappe.session.user}）"
                    
            frappe.msgprint(success_msg, title=f"{ContactConstants.LOG_TITLE} - 更新成功")

            log_message = f"{ghl_id} 账户插入成功!\n{doc_dict}\n{_t}"
            frappe.logger().error(log_message)
        return out

    except Exception as e:
        error_msg = f"更新 GoHighLevel账户 {doc.name}时出错：{str(e)},{traceback.format_exc()}"
        frappe.logger().error(f"{error_msg}")  # 记录错误日志
        #frappe.throw(error_msg)  # 抛出异常
   

def on_gohighlevel_contacts_on_trash(doc: Document, method=None):
    '''绑定glh 联系人 删除事件'''
    try:
        ghl_id = doc.custom_gohighlevel_contact_id
        ghl_locationid =doc.custom_custom_gohighlevel_locationid
        if ghl_locationid:
            ghc = get_hl_client(ghl_locationid)

            #_t =ghc.contacts.delete_contact(ghl_id)
        _t =ghc.contacts.delete_contact(ghl_id)
        frappe.logger().error(f"{ghl_id} 账户删除成功!,{_t}")  # 记录错误日志

    except Exception as e:
        error_msg = f"删除 GoHighLevel账户 {doc.name}时出错：{str(e)},{traceback.format_exc()}"
        frappe.logger().error(f"{error_msg}")  # 记录错误日志
        frappe.throw(error_msg)  # 抛出异常

@frappe.whitelist(allow_guest=True)
def on_gohighlevel_contacts_update_test(doc_name:str):
    """'''绑定glh 联系人  更新事件'''"""
    # 示例：校验客户信用额度
    doc = frappe.get_doc("Contact",doc_name)
    try:
        ghl_id = doc.custom_gohighlevel_contact_id
        ghl_locationid =doc.custom_custom_gohighlevel_locationid
        #if ghl_locationid:
        # 没有GHL信息直接退出，绝不报错
        if not ghl_id or not ghl_locationid:
            return None

        # 获取GHL客户端
        ghc = get_hl_client(ghl_locationid)
        
        
        out = {
            'firstName':    doc.first_name,
            'lastName':     doc.last_name ,#
            'name':         doc.full_name, #全名
            'email':        doc.email_id, #email
            'phone':        doc.phone,    #手机
        }

        if doc.address :
            addr_doc = frappe.get_doc('Address',doc.address)

            out['address1'] = doc.address #地址
            out['city'] = addr_doc.city         #城市
            out['state'] = addr_doc.state       #省洲
            out['postalCode'] = addr_doc.pincode #邮政编码
        
            if addr_doc.country:
                country_doc = frappe.get_doc('Country',addr_doc.country)

            if country_doc:
                out['country'] = country_doc.code.upper() # #国家代码

        out = {k:v for k,v in out.items() if v is not None or v != ''}

        out['locationId'] = ghl_locationid

        update = UpdateContactDto(**out)

        with reusable_async_loop() as loop:
            _t = loop.run_until_complete(ghc.contacts.update_contact(ghl_id,update.model_dump(exclude_none=True)))
        
            out['_t'] = _t
            

                #_t =ghc.contacts.delete_contact(ghl_id)
            #frappe.logger().error(f"{ghl_id,} 账户更新成功!\n{doc.as_dict()}\n{_t}")  # 记录错误日志
            doc_dict = doc.as_dict()
            out['doc'] = doc_dict

            log_message = f"{ghl_id} 账户插入成功!\n{doc_dict}\n{_t}"
            frappe.logger().error(log_message)
        return out

    except Exception as e:
        error_msg = f"更新 GoHighLevel账户 {doc.name}时出错：{str(e)},{traceback.format_exc()}"
        frappe.logger().error(f"{error_msg}")  # 记录错误日志
        #frappe.throw(error_msg)  # 抛出异常