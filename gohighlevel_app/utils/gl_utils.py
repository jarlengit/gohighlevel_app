
import frappe
import json
import asyncio
from contextlib import contextmanager
from typing import Dict, Any, Optional
from deepdiff import DeepDiff
from frappe.model.document import Document
from highlevel import HighLevel
from highlevel.services.contacts.models import UpdateContactDto, UpsertContactDto


# ======================== 常量定义 ========================
class ContactConstants:
    """联系人同步常量配置"""
    # 需要解析为列表的字段
    LIST_FIELDS = ["tags", "customfields"]
    # 需要解析为字典的字段
    DICT_FIELDS = ["dndsettings"]
    # 日志标题前缀
    LOG_TITLE = "GoHighLevel联系人同步"
    # 客户端初始化失败提示
    CLIENT_INIT_FAILED = "初始化GoHighLevel客户端失败"
fields = (
    "id", "locationId", "contactName", "firstName", "lastName", "firstNameRaw", "lastNameRaw", "companyName", "email", "phone",
    "dnd", "dndSettings", "type", "source", "assignedTo", "city", "state", "postalCode", "address1", "dateAdded", "dateUpdated",
    "dateOfBirth", "businessId", "tags",  "followers", "country", "website", "timezone", "profilePhoto", "additionalEmails",
    "customFields", "startAfter"
)
fields_map = frappe._dict({d:d.lower() for d in fields if d != d.lower()})
doc_fields_map = frappe._dict({d.lower():d for d in fields if d != d.lower()})

# ======================== 异步工具函数 ========================
@contextmanager
def reusable_async_loop():
    """可复用的异步循环上下文管理器（增加容错处理）"""
    loop = None
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        yield loop
    except Exception as e:
        frappe.logger().error(f"{ContactConstants.LOG_TITLE} - 异步循环创建失败: {str(e)}")
        raise
    finally:
        if loop:
            try:
                loop.close()
            except Exception as e:
                frappe.logger().error(f"{ContactConstants.LOG_TITLE} - 异步循环关闭失败: {str(e)}")
        asyncio.set_event_loop(None)


def get_highlevel_client(location_id) -> Optional[HighLevel]:
    """
    获取GoHighLevel客户端实例（优化版）
    :return: 初始化成功返回HighLevel实例，失败返回None
    """
    try:
        # 严格校验location_id
        if not location_id or not isinstance(location_id, str):
            frappe.logger().error(
                f"{ContactConstants.LOG_TITLE} - locationId无效: {location_id}"
            )
            return None

        # 获取私域token
        private_token = frappe.get_value(
            "GoHighLevel_Set",
            {"check": 1, "name": location_id},
            "private_integration_token"
        )

        if not private_token:
            frappe.logger().error(
                f"{ContactConstants.LOG_TITLE} - 未找到有效token:{private_token} locationId={location_id}"
            )
            return None

        return HighLevel(private_integration_token=private_token)

    except Exception as e:
        error_msg = f"{ContactConstants.LOG_TITLE} - 客户端初始化异常: {str(e)}, locationId={locationid}"
        frappe.logger().error(error_msg)
        frappe.throw(error_msg)



def get_contact_doc(data: Dict[str, Any]) -> Dict[str, Any]:
    """根据字段定义解析联系人数据
    {'id': 'Dg4BsoqIKLirUfQivfV2', 'locationId': 'Rft5PXFLw9e6vhedtZ86', 
    'contactName': '(example) jordan smith', 
    'firstName': '(example) jordan', 
    'lastName': 'smith', 
    'firstNameRaw': '(example) jordan', 
    'lastNameRaw': 'smith', 
    'companyName': "(Example) MacLaren's Pub", 
    'email': 'jordan.smith@example.com', 
    'phone': None, 'dnd': True, 
    'dndSettings': {'Call': {'status': 'inactive', 'code': '103'}, 'Email': {'status': 'inactive', 'code': '103'}, 
    'SMS': {'status': 'inactive', 'code': '103'}}, 
    'type': 'customer', 
    'source': None, 
    'assignedTo': None, 
    'city': None, 
    'state': None, 
    'postalCode': None, 
    'address1': None, 
    'dateAdded': '2026-03-05T14:20:36.702Z', 
    'dateUpdated': '2026-03-20T16:49:03.195Z', 
    'dateOfBirth': None, 
    'businessId': '69a9912c5ab8294414041a1a', 
    'tags': ['follow-up'], 
    'followers': [], 
    'country': 'US', 
    'website': None, 'timezone': None,
      'profilePhoto': None, 
      'additionalEmails': [{'validEmailDate': None, 'email': 
'jordan.office@example.com'}, 
{'validEmailDate': None, 'email': 'jordan@corporate.net'}, 
{'validEmailDate': None, 'email': 'jsmith@email.com'}], 
'customFields': [], 'startAfter': [1772720436702, 'Dg4BsoqIKLirUfQivfV2']}

    """
    frappe.logger().error(f"gl数据解析:{data}")  #记录日志

    
    doc = {'doctype': 'Contact'}
    key_dict = {
        'contactName':'full_name',
        'firstName':'first_name',
        'lastName':'last_name',
        'companyName':'company_name',
        'email':'email_id',
        'phone':'phone',
        'type':'contact_type',
        'tags':'tags',
        'dateOfBirth':'date_of_birth',
        'website':'website',
        'additionalEmails':'email_ids',
        'followers':'followers',
        'profilePhoto':'image',    #头像
        'custom_location_id_token':"custom_location_id_token"
    }

    #关键字段不为空才进行赋值
    doc.update({key_dict.get(k,k):v for k,v in data.items() if k in key_dict and v is not None})
    
    doc['custom_gohighlevel_contact_id'] = data.get('id')
    if data.get('email'):
        doc['email_id'] = data.get('email')
        doc['email_ids'] = []
        doc['email_ids'].append({'email_id':data.get('email'), 'is_primary':1})
        
        for i in data.get('additionalEmails',[]):
            doc['email_ids'].append({'email_id':i.get('email'), 'is_primary':0})
    if data.get('phone'):
        doc['phone_nos'] = [{'phone': data.get('phone'),'is_primary_phone':True}]
     
    frappe.logger().error(f"gl数据解析2:{doc}")  #记录日志
    return doc


def get_address_doc(data: Dict[str, Any]) -> Dict[str, Any]:
    """根据字段定义解析联系人数据"""
    doc = {'doctype': 'Address'}

    key_dict = {
        'address1':'address_line1',
        'city':'city',
        'county':'county',
        'state':'state',
        'postalCode':'pincode',
        'country':'country',
        'email':'email_id',
        'phone':'phone',
        #'website':'website',
        #'locationId':'location_id'
        
    }

    #关键字段不为空才进行赋值
    doc.update({key_dict.get(k,k):v for k,v in data.items() if k in key_dict and v is not None})
    
    if doc.get('country') is not None:
        doc['country'] =  doc.get('country').lower() 
    else:
        doc['country'] = 'cn'
    
    doc['custom_box'] = 1
    
    doc['address_title'] = doc.get('address_line1') if doc.get('address_line1') else '地址信息'
    
    if doc.get('city') is  None:
        doc['city'] = '*'
    
    return doc

def get_contact_lst(location_id: str) -> Optional[Dict[str, Any]]:
    """根据location_id和contact_id获取联系人数据"""
    ghc = get_highlevel_client(location_id)
    if not ghc:
        return None

    try:
        with reusable_async_loop() as loop:
            start_after_id = None
            start_after = None
            contacts_all = []
            
            
            while True:
                #contacts = await hl_client.contacts.get_contacts(location_id=location_id,limit=1)
                #tags = contacts.get("meta",{}).get('total')
                #contacts_all.extend(  contacts.get('contacts'))
                query = {
                    'location_id':location_id,
                    'limit':100,
                }
                # 非第一页，添加分页参数（驼峰命名，匹配接口要求）
                if start_after and start_after_id:
                    query["start_after"] = start_after
                    query["start_after_id"] = start_after_id
                
                contacts = loop.run_until_complete(ghc.contacts.get_contacts(**query))

                #print(contacts)
                if contacts.get('contacts') :
                    contacts_all.extend(  contacts.get('contacts'))
                
               
                meta = contacts.get("meta", {})
                total = meta.get("total", 0)  # 总记录数
                if total == 0:
                    frappe.msgprint(f"记录无联系人数据")
                    return None
                new_start_after = meta.get("startAfter")  # 下一页时间戳
                new_start_after_id = meta.get("startAfterId")  # 下一页ID
                has_next_page = bool(meta.get("nextPageUrl"))  # 是否有下一页

                # 终止条件：无下一页 或 已获取全部数据
                if not has_next_page or len(contacts_all) >= total:
                    break
                # 更新分页参数，进入下一轮循环
                start_after = new_start_after
                start_after_id = new_start_after_id

             
            return contacts_all

    except Exception as e:
        frappe.logger().error(f"{ContactConstants.LOG_TITLE} - 获取联系人数据异常:{str(e)}, {frappe.traceback.format_exc()}, locationId={location_id}, {all_contacts}")
        return None
'''
def upinsert_contact_doc(data: Dict[str, Any]) -> Dict[str, Any]:
    """根据字段定义解析联系人数据"""
    doc = get_contact_doc(data)             #提取联系人文档数据
    address_doc = get_address_doc(data)     #提取地址文档数据
    frappe.logger().error(f"gl数据解析完成1:{data}")  #记录日志

    frappe.logger().error(f"gl数据解析完成2:{address_doc} \n\n{doc}")  #记录日志
    #地址数据不为空才进行创建
    if address_doc.get("address_line1"):
        address_doc['locationid'] = data.get('locationId')
        #更新国家代码,提取文档name
        address_doc['country'] = frappe.db.exists('Country',{'code': address_doc.get('country')})
        #frappe.get_value('Country',  filters={'code': address_doc.get('country')},fieldname='name')
        frappe.logger().error(f"记录地址数据:{address_doc}")  # 记录错误日志

        #处理地址数据，先判断是否存在相同locationid和address_line1的地址记录，如果存在则更新，不存在则创建
        if address :=frappe.db.exists(address_doc['doctype'], {'locationid': data.get('locationId')}):
            #存在
            address_doc.pop("name", None)
            address_doc = frappe.get_doc(address_doc['doctype'], address)
            address_doc.update(address_doc)
            address_doc.save(ignore_permissions=True)
        else:
            #不存在
            address_doc = frappe.new_doc(address_doc['doctype']).update(address_doc).insert(ignore_permissions=True)
        frappe.db.commit()
        doc['address'] = address_doc.name
        doc['custom_custom_gohighlevel_locationid'] = data.get('locationId')

    #更新联系人数据，先判断是否存在相同locationid和name的联系人记录，如果存在则更新，不存在则创建
    if ct :=frappe.db.get_value('Contact', {'custom_gohighlevel_contact_id': doc.get('custom_gohighlevel_contact_id')},'full_name'):
        #存在
        doc.pop("name", None)
        doc = frappe.get_doc(doc['doctype'], ct).update(doc).save(ignore_permissions=True)
    else:
        #不存在
        doc = frappe.new_doc(doc['doctype']).update(doc).insert(ignore_permissions=True,ignore_if_duplicate=True)
    frappe.logger().info(f"new_doc数据:{doc.as_dict()}")  #记录
    frappe.db.commit()

    return doc



'''


def upinsert_contact_doc(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    根据字段定义解析联系人数据，执行更新或插入操作 (Upsert)
    优化点：
    1. 修复了 update/insert 链式调用的致命错误
    2. 分离了地址与联系人的处理逻辑
    3. 移除了不必要的手动 commit
    4. 规范了日志级别
    5. 增加了事务一致性处理
    """
    
    # 1. 提取基础数据
    contact_data = get_contact_doc(data)
    address_data = get_address_doc(data) # 修正了原代码的拼写错误 dddress -> address
    
    # 使用 debug 级别记录调试信息，避免污染 error 日志
    frappe.logger().debug(f"解析完成 - 原始数据: {data}")
    frappe.logger().debug(f"解析完成 - 地址数据: {address_data}, 联系人数据: {contact_data}")

    location_id = data.get('locationId')
    address_name = None

    # 2. 处理地址逻辑
    if address_data and address_data.get("address_line1"):
        address_data['locationid'] = location_id
        
        # 优化国家代码查找：如果没找到，保持原样或后续由系统校验
        country_code = address_data.get('country')
        if country_code:
            country_name = frappe.db.exists('Country', {'code': country_code})
            if country_name:
                address_data['country'] = country_name

        # 定义查找条件
        address_doctype = address_data.get('doctype')
        existing_address_name = frappe.db.exists(address_doctype, {'locationid': location_id})

        try:
            if existing_address_name:
                # 更新逻辑：获取文档 -> 更新字段 -> 保存
                address_doc = frappe.get_doc(address_doctype, existing_address_name)
                address_doc.update(address_data)
                address_doc.save(ignore_permissions=True)
            else:
                # 插入逻辑：新建文档 -> 赋值 -> 插入
                # 注意：pop 掉 doctype，防止 new_doc 参数冲突
                address_data.pop("doctype", None) 
                address_doc = frappe.new_doc(address_doctype)
                address_doc.update(address_data)
                address_doc.insert(ignore_permissions=True)
            
            address_name = address_doc.name
            frappe.logger().debug(f"地址处理完成: {address_name}")
        except Exception as e:
            frappe.log_error(f"地址保存失败 (LocationID: {location_id}): {str(e)}")
            # 根据业务需求决定是否抛出异常中断流程
            # raise

    # 3. 处理联系人逻辑
    contact_doctype = contact_data.get('doctype', 'Contact')
    contact_id = contact_data.get('custom_gohighlevel_contact_id')
    
    # 关联地址和 LocationID (如果地址生成成功)
    if address_name:
        contact_data['address'] = address_name
        contact_data['custom_custom_gohighlevel_locationid'] = location_id

    # 查找现有联系人
    existing_contact_name = frappe.db.get_value(
        'Contact', 
        {'custom_gohighlevel_contact_id': contact_id}, 
        'name' # 注意：原代码取的是 full_name，这里修正为取 name，否则 get_doc 会报错
    )

    final_contact_doc = None
    try:
        if existing_contact_name:
            # 更新逻辑
            final_contact_doc = frappe.get_doc(contact_doctype, existing_contact_name)
            # 防止更新覆盖 name 等核心字段
            contact_data.pop("name", None)
            contact_data.pop("doctype", None)
            
            final_contact_doc.update(contact_data)
            final_contact_doc.save(ignore_permissions=True)
        else:
            # 插入逻辑
            contact_data.pop("doctype", None)
            final_contact_doc = frappe.new_doc(contact_doctype)
            final_contact_doc.update(contact_data)
            final_contact_doc.insert(ignore_permissions=True, ignore_if_duplicate=True)

        frappe.logger().info(f"联系人处理完成: {final_contact_doc.name}")
    except Exception as e:
        frappe.log_error(f"联系人保存失败 (GHL ID: {contact_id}): {str(e)}")
        raise # 联系人通常是主数据，失败建议抛出异常

    return final_contact_doc