# Copyright (c) 2026, jarlen and contributors
# For license information, please see license.txt

import frappe
import json
import asyncio
from contextlib import contextmanager
from typing import Dict, Any, Optional
from deepdiff import DeepDiff
from frappe.model.document import Document
from highlevel import HighLevel
from highlevel.services.contacts.models import UpdateContactDto, UpsertContactDto
from gohighlevel_app.utils.gl_utils import ContactConstants,fields_map,reusable_async_loop,get_highlevel_client

# ======================== 文档类 ========================
class GoHighLevel_contacts(Document):
    """GoHighLevel联系人同步文档类"""

    def _parse_json_fields(self, doc_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        统一解析JSON格式字段（增加容错处理）
        :param doc_data: 文档字典数据
        :return: 解析后的字典
        """
        # 解析列表类型字段
        for field in ContactConstants.LIST_FIELDS:
            try:
                doc_data[field] = json.loads(doc_data.get(field) or "[]")
                if not isinstance(doc_data[field], list):
                    doc_data[field] = []
                    frappe.logger().warning(
                        f"{ContactConstants.LOG_TITLE} - 字段{field}解析后非列表，已重置为空列表: {doc_data.get(field)}"
                    )
            except json.JSONDecodeError as e:
                doc_data[field] = []
                frappe.logger().error(
                    f"{ContactConstants.LOG_TITLE} - 字段{field}JSON解析失败: {str(e)}, 值={doc_data.get(field)}"
                )

        # 解析字典类型字段
        for field in ContactConstants.DICT_FIELDS:
            try:
                doc_data[field] = json.loads(doc_data.get(field) or "{}")
                if not isinstance(doc_data[field], dict):
                    doc_data[field] = {}
                    frappe.logger().warning(
                        f"{ContactConstants.LOG_TITLE} - 字段{field}解析后非字典，已重置为空字典: {doc_data.get(field)}"
                    )
            except json.JSONDecodeError as e:
                doc_data[field] = {}
                frappe.logger().error(
                    f"{ContactConstants.LOG_TITLE} - 字段{field}JSON解析失败: {str(e)}, 值={doc_data.get(field)}"
                )
        #将字段转换 文档字段转为gl平台的字段
        doc_data = {fields_map.get(k,k):v for k,v in doc_data.items() }
        return doc_data

    def _sync_to_highlevel_create(self):
        """
        同步创建联系人到GoHighLevel平台（提取重复逻辑）
        """
        hl_client = get_highlevel_client(self.locationid)
        if not hl_client:
            frappe.throw(f"{ContactConstants.CLIENT_INIT_FAILED}，无法创建联系人")

        try:
            with reusable_async_loop() as loop:
                # 处理文档数据
                doc_data = self.as_dict()
                doc_data.pop("name", None)  # 安全删除name字段
                doc_data.pop("locationid", None)  # 安全删除locationid字段
                parsed_data = self._parse_json_fields(doc_data)
                frappe.logger().error(f"插入数据1: {parsed_data}")

                # 构建DTO并调用接口
                upsert_dto = UpsertContactDto(**parsed_data,locationId=self.locationid)
                request_data = upsert_dto.model_dump(exclude_none=True)
                frappe.logger().error(f"插入数据2: {request_data}")
                response = loop.run_until_complete(
                    hl_client.contacts.upsert_contact(request_body=request_data)
                )
                # 处理响应
                if response.get("new", False) or response.get("succeded", False) :  # 修正原代码拼写错误succeded→succeeded
                    new_name = response.get("contact", {}).get("id")
                    if new_name:
                        # 2. 检查新名称是否已存在（避免冲突）
                        if frappe.db.exists(self.doctype, new_name):
                            frappe.throw(f"重命名失败：名称 {new_name} 已存在！")

                        # 3. 执行重命名
                        # 参数说明：文档类型、原名称、新名称、是否忽略权限、是否合并
                        frappe.rename_doc( self.doctype, self.name,new_name,merge=False)
                        self.name = new_name
                        frappe.logger().error(f"文档已重命名为：{new_name}")
                    
                        success_msg = (
                            f"联系人 {new_name} 已同步创建到GoHighLevel（操作人：{frappe.session.user}）"
                        )
                        frappe.msgprint(success_msg, title=f"{ContactConstants.LOG_TITLE} - 创建成功")
                        frappe.logger().info(success_msg)
                else:
                    error_msg = f"{ContactConstants.LOG_TITLE} - 创建失败: {response}, contact_data={request_data}"
                    frappe.logger().error(error_msg)
                    frappe.throw(error_msg)

                '''
                # 处理响应
                if response.get("succeded", False):  # 修正原代码拼写错误succeded→succeeded
                    contact_id = response.get("contact", {}).get("id")
                    if contact_id:
                        self.name = contact_id
                        success_msg = (
                            f"联系人 {contact_id} 已同步创建到GoHighLevel（操作人：{frappe.session.user}）"
                        )
                        frappe.msgprint(success_msg, title=f"{ContactConstants.LOG_TITLE} - 创建成功")
                        frappe.logger().info(success_msg)
                else:
                    error_msg = f"{ContactConstants.LOG_TITLE} - 创建失败: {response}, contact_data={request_data}"
                    frappe.logger().error(error_msg)
                    frappe.throw(error_msg)
                '''

        except Exception as e:
            error_msg = f"{ContactConstants.LOG_TITLE} - 创建联系人异常: {str(e)}, contact_id={self.name}\n详细报错内容:{frappe.get_traceback()}"
            frappe.logger().error(error_msg)
            frappe.throw(error_msg)

    def _sync_to_highlevel_update(self):
        """
        同步更新联系人到GoHighLevel平台（提取重复逻辑）
        """
        hl_client = get_highlevel_client(self.locationid)
        
        if not hl_client:
            frappe.throw(f"{ContactConstants.CLIENT_INIT_FAILED}，无法更新联系人")

        try:
            with reusable_async_loop() as loop:
                # 处理文档数据
                doc_data = self.as_dict()
                doc_data.pop("name", None)
                parsed_data = self._parse_json_fields(doc_data)

                # 构建DTO并调用接口
                update_dto = UpdateContactDto(**parsed_data)
                request_data = update_dto.model_dump(exclude_none=True)
                response = loop.run_until_complete(
                    hl_client.contacts.update_contact(contact_id=self.name, request_body=request_data)
                )

                # 处理响应 succeded
                if response.get("succeded", False):  # 修正拼写错误
                    success_msg = (
                        f"联系人 {self.name} 已同步更新到GoHighLevel（操作人：{frappe.session.user}）"
                    )
                    frappe.msgprint(success_msg, title=f"{ContactConstants.LOG_TITLE} - 更新成功")
                    frappe.logger().info(success_msg)
                else:
                    error_msg = f"{ContactConstants.LOG_TITLE} - 更新失败: {response}, contact_id={self.name}"
                    frappe.logger().error(error_msg)
                    frappe.throw(error_msg)

        except Exception as e:
            error_msg = f"{ContactConstants.LOG_TITLE} - 更新联系人异常: {str(e)}, contact_id={self.name}"
            
            frappe.logger().error(error_msg)
            #frappe.throw(error_msg)
            #gl平台创建新的记录,
            self._sync_to_highlevel_create()

    # ======================== Frappe钩子方法 ========================
    def before_insert(self):
        """创建前钩子：同步创建到GoHighLevel"""
        #self._sync_to_highlevel_create()
        pass

    def on_update(self):
        """更新后钩子：同步更新到GoHighLevel"""
        self._sync_to_highlevel_update()

    def on_trash(self):
        """软删除钩子：删除GoHighLevel平台联系人"""
        msg = ""
        hl_client = get_highlevel_client(self.locationid)

        try:
            if hl_client:
                with reusable_async_loop() as loop:
                    try:
                        # 先查询联系人是否存在
                        contact_detail = loop.run_until_complete(
                            hl_client.contacts.get_contact(contact_id=self.name)
                        )
                        if contact_detail:
                            # 执行删除
                            delete_resp = loop.run_until_complete(
                                hl_client.contacts.delete_contact(contact_id=self.name)
                            )
                            if delete_resp.get("succeded", False):
                                msg = f"联系人 {self.name} 已从GoHighLevel平台删除（无法恢复）"
                            else:
                                msg = f"联系人 {self.name} 删除失败: {delete_resp}"
                        else:
                            msg = f"联系人 {self.name} 在GoHighLevel平台不存在，无需删除"
                    except Exception as e:
                        msg = f"联系人 {self.name} 删除异常（平台可能已删除）: {str(e)}"
            else:
                msg = f"{ContactConstants.CLIENT_INIT_FAILED}，无法删除平台联系人: {self.name}"
                frappe.logger().error(msg)

            frappe.msgprint(msg)

        except Exception as e:
            error_msg = f"{ContactConstants.LOG_TITLE} - 删除联系人异常: {str(e)}, contact_id={self.name}"
            frappe.logger().error(error_msg)
            frappe.throw(error_msg)

    def after_delete(self):
        """物理删除钩子：记录日志"""
        log_msg = f"联系人 {self.name} 已物理删除（操作人：{frappe.session.user}，时间：{frappe.utils.now()}）"
        frappe.log_error(message=log_msg, title=f"{ContactConstants.LOG_TITLE} - 物理删除日志")
        frappe.msgprint(f"联系人 {self.name} 已彻底删除，无法恢复！")

        """核心重命名逻辑"""
        try:
            # 1. 定义重命名规则（根据你的业务需求修改）
            # 示例规则：DOC-20240314-001
            #获取 new_name
            hl_client = get_highlevel_client(self.locationid)

            if not hl_client:
                frappe.throw(f"{ContactConstants.CLIENT_INIT_FAILED}，无法创建联系人")

            with reusable_async_loop() as loop:
                doc_data = self.as_dict()
                doc_data.pop("name", None)  # 安全删除name字段
                parsed_data = self._parse_json_fields(doc_data)

                # 构建DTO并调用接口
                upsert_dto = UpsertContactDto(**parsed_data,locationId=self.locationid)
                request_data = upsert_dto.model_dump(exclude_none=True)
                response = loop.run_until_complete( hl_client.contacts.upsert_contact(request_body=request_data) )
                # 处理响应
                if response.get("new", False):  # 修正原代码拼写错误succeded→succeeded
                    new_name = response.get("contact", {}).get("id")
                    if new_name:
                        # 2. 检查新名称是否已存在（避免冲突）
                        if frappe.db.exists(self.doctype, new_name):
                            frappe.throw(f"重命名失败：名称 {new_name} 已存在！")

                        # 3. 执行重命名
                        # 参数说明：文档类型、原名称、新名称、是否忽略权限、是否合并
                        frappe.rename_doc( self.doctype, self.name,new_name,merge=True)
                        self.name = new_name
                        frappe.logger().error(f"文档已重命名为：{new_name}")
                    
                        success_msg = (
                            f"联系人 {contact_id} 已同步创建到GoHighLevel（操作人：{frappe.session.user}）"
                        )
                        frappe.msgprint(success_msg, title=f"{ContactConstants.LOG_TITLE} - 创建成功")
                        frappe.logger().info(success_msg)
                else:
                    error_msg = f"{ContactConstants.LOG_TITLE} - 创建失败: {response}, contact_data={request_data}"
                    frappe.logger().error(error_msg)
                    frappe.throw(error_msg)

        except Exception as e:
            frappe.log_error(f"重命名失败：{str(e)}", "文档重命名错误")
            frappe.throw(f"重命名失败：{str(e)}")