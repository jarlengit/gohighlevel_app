# Copyright (c) 2026, jarlen and contributors
# For license information, please see license.txt

from highlevel import HighLevel
from highlevel.services.contacts.models import UpdateContactDto ,UpsertContactDto
import frappe,json
from deepdiff import DeepDiff
from frappe.model.document import Document
import asyncio
from contextlib import contextmanager

@contextmanager
def reusable_async_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        yield loop
    finally:
        loop.close()
        asyncio.set_event_loop(None)



class GoHighLevel_contacts(Document):

	def get_HighLevel(self):
		'''获取gl客户端'''
		try:
			hl_client = None
			locationId = self.locationid

			if locationId:
				frappe.logger().error(f"参数,locationId={locationId}") #记录处理结果日志

				private_integration_token = frappe.get_value (
						"GoHighLevel_Set",          					# 第1个参数：doctype
						{"check": 1,"locationId": locationId},          	# 第2个参数：filters（字典）
						"private_integration_token" 					# 
					)
				if private_integration_token:
					hl_client = HighLevel( private_integration_token=private_integration_token)  
				else:
					frappe.logger().error(f"私域token private_integration_token= {private_integration_token} 参数无效,locationId={locationId}") #记录处理结果日志

			else:
				frappe.logger().error(f"locationId:{locationId} 参数无效") #记录处理结果日志

			return hl_client
		except Exception as e:
			msg = f"{e},locationId 参数无效,{locationId}"
			
			frappe.logger().error(msg) #记录处理结果日志

			frappe.throw(msg)  # 抛出异常


	def on_trash(self):
		"""
		软删除（移入回收站）时触发（核心删除事件）
		替代 hooks 中的 on_trash 绑定，框架自动调用
		"""
		try:
			'''处理GoHighLevel的webhook事件，删除事件'''
			hl_client = self.get_HighLevel()
			#执行异步删除函数(删除远程数据)
			if hl_client:
				with reusable_async_loop() as loop:
					#查询文档
					try:
						response = loop.run_until_complete(hl_client.contacts.get_contact(contact_id = self.name))
						#执行删除操作
						if response:
							response = loop.run_until_complete(hl_client.contacts.delete_contact(contact_id = self.name))
							if response.get('succeded',False):
								msg = f"联系人 {self.name} 已彻底删除，无法恢复！{response}"
						else:
							msg = f"联系人 {self.name} 平台不存在,无需删除!！{response}"
					except Exception as e:
						msg = f"联系人 {self.name} 平台已彻底删除，！"						
						
			else:
				#msg = f"删除GoHighLevel{self.name} 记录失败,token无效!" 

				msg = f"删除GoHighLevel{self.name} 记录失败, 初始化gl客户端失败!"
				frappe.logger().error(msg) #记录处理结果日志
				frappe.msgprint(msg)

		except Exception as e:
			error_msg = f"删除GoHighLevel联系人记录失败: {str(e)},contact_id={self.name}"
			frappe.logger().error(error_msg)  # 记录错误日志
			frappe.throw(error_msg)  # 抛出异常

	def after_delete(self):
		"""
		物理删除（彻底删除）时触发
		仅当调用 doc.delete(force=True)/清空回收站时执行
		"""
		# 物理删除后同步清理外部数据（示例）
		frappe.log_error(
			message=f"联系人 {self.name} 已物理删除（操作人：{frappe.session.user}）",
			title="联系人物理删除日志"
		)
		frappe.msgprint(f"联系人 {self.name} 已彻底删除，无法恢复！")

	def __update(self):
		''' 内部函数,gl更新'''
		hl_client = self.get_HighLevel()
		try:
			#执行异步删除函数(删除远程数据)
			if hl_client:
				with reusable_async_loop() as loop:
					doc_data = self.as_dict()
					del doc_data['name']
					list_fields = ['tags', 'customFields']
					for field in list_fields:
						# 确保字段值为列表（空值则转空列表）
						doc_data[field] = json.loads(doc_data[field] or "[]")
						# 额外校验：防止解析后仍不是列表（如JSON字符串格式错误）
						if not isinstance(doc_data[field], list):
							raise ValueError(f"字段{field}解析后不是列表，当前值：{doc_data[field]}")
					
					# 3. 处理dndSettings字段（转为字典，核心修复！）
					dnd_field = 'dndSettings'
					if dnd_field in doc_data:
						# 空值则转空字典，非空则解析为字典
						doc_data[dnd_field] = json.loads(doc_data[dnd_field] or "{}")
						# 额外校验：确保是字典
						if not isinstance(doc_data[dnd_field], dict):
							raise ValueError(f"字段{dndSettings}解析后不是字典，当前值：{doc_data[dnd_field]}")
					# 4. 实例化DTO并过滤空值
					data = UpdateContactDto(**doc_data)
					data_dict = data.model_dump(exclude_none=True)

					# 调用更新接口
					response = loop.run_until_complete( hl_client.contacts.update_contact(contact_id=self.name,request_body=data_dict))
					if response.get('succeded',False):
						frappe.msgprint(
							f"联系人 {self.name} 已更新（操作人：{frappe.session.user},更新内容:{response.get('contact')}）",
							title=f"gl平台同步更新成功")
			
		except Exception as e:
			error_msg = f"更新GoHighLevel联系人记录失败: {str(e)},contact_id={self.name}"
			frappe.logger().error(error_msg)  # 记录错误日志
			frappe.throw(error_msg)  # 抛出异常

	def __insert(self):
		'''插入操作'''
		hl_client = self.get_HighLevel()
		try:
			#执行异步删除函数(删除远程数据)
			if hl_client:
				with reusable_async_loop() as loop:
					doc_data = self.as_dict()
					del doc_data['name']
					list_fields = ['tags', 'customFields']
					for field in list_fields:
						# 确保字段值为列表（空值则转空列表）
						doc_data[field] = json.loads(doc_data[field] or "[]")
						# 额外校验：防止解析后仍不是列表（如JSON字符串格式错误）
						if not isinstance(doc_data[field], list):
							raise ValueError(f"字段{field}解析后不是列表，当前值：{doc_data[field]}")
					
					# 3. 处理dndSettings字段（转为字典，核心修复！）
					dnd_field = 'dndSettings'
					if dnd_field in doc_data:
						# 空值则转空字典，非空则解析为字典
						doc_data[dnd_field] = json.loads(doc_data[dnd_field] or "{}")
						# 额外校验：确保是字典
						if not isinstance(doc_data[dnd_field], dict):
							raise ValueError(f"字段{dndSettings}解析后不是字典，当前值：{doc_data[dnd_field]}")
					# 4. 实例化DTO并过滤空值
					data = UpsertContactDto(**doc_data)
					data_dict = data.model_dump(exclude_none=True)

					# 调用更新接口
					response = loop.run_until_complete( hl_client.contacts.upsert_contact(request_body=data_dict))
					if response.get('succeded',False):
						self.name = response.get('contact',{}).get('id')

						frappe.msgprint(
							f"联系人 {self.name} 已插入成功（操作人：{frappe.session.user},更新内容:{response.get('contact')}）",
							title=f"gl平台同步插入成功")
			
		except Exception as e:
			error_msg = f"更新GoHighLevel联系人记录失败: {str(e)},contact_id={self.name}"
			frappe.logger().error(error_msg)  # 记录错误日志
			frappe.throw(error_msg)  # 抛出异常


	def on_update(self):
		'''同步修改
		将修改内容提交给gl平台
		'''
		self.__update()
	
	def before_naming(self):
		'''命名阶段'''
		self.__insert()
		
	def before_insert(self):
		'''创建同步'''
		hl_client = self.get_HighLevel()
		try:
			#执行异步删除函数(删除远程数据)
			if hl_client:
				with reusable_async_loop() as loop:
					doc_data = self.as_dict()
					del doc_data['name']
					for k in [ 'dndSettings','tags','customFields']:
						doc_data[k] = json.loads(doc_data[k] or "[]")
					data = UpsertContactDto(**doc_data)
					data_dict = data.model_dump(exclude_none=True) 

					# 调用更新接口
					response = loop.run_until_complete( hl_client.contacts.upsert_contact(request_body=data_dict))
					if response.get('succeded',False):
						self.name = response.get('contact',{}).get('id')
						frappe.msgprint(
							f"联系人 {self.name} 已插入成功（操作人：{frappe.session.user},更新内容:{response.get('contact')}）",
							title=f"gl平台同步插入成功")
			
		except Exception as e:
			error_msg = f"更新GoHighLevel联系人记录失败: {str(e)},contact_id={self.name}"
			frappe.logger().error(error_msg)  # 记录错误日志
			frappe.throw(error_msg)  # 抛出异常

	def before_insert(self):
		'''插入后操作,等价于更新操作'''
		self.__update()
	
	def rename_based_on_rule(self):
		"""核心重命名逻辑"""
		try:
			# 1. 定义重命名规则（根据你的业务需求修改）
			# 示例规则：DOC-20240314-001
			#获取 new_name
			hl_client = self.get_HighLevel()

			# 2. 检查新名称是否已存在（避免冲突）
			if frappe.db.exists("Custom DocType", new_name):
				frappe.throw(f"重命名失败：名称 {new_name} 已存在！")

			# 3. 执行重命名
			# 参数说明：文档类型、原名称、新名称、是否忽略权限、是否合并
			frappe.rename_doc(
				doctype=self.doctype,
				old=self.name,
				new=new_name,
				ignore_permissions=True,
				merge=False
			)

			# 4. 更新当前文档对象的名称
			self.name = new_name
			frappe.msgprint(f"文档已重命名为：{new_name}", alert=True)

		except Exception as e:
			frappe.log_error(f"重命名失败：{str(e)}", "文档重命名错误")
			frappe.throw(f"重命名失败：{str(e)}")