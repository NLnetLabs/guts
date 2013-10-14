#!/usr/bin/env python
import os
import sqlite3

def create_db():
	db_filename = 'Measurements.db'
	db_is_new = not os.path.exists(db_filename)
	conn = sqlite3.connect(db_filename)
	cursor = conn.cursor()
	db_spec = []
	db_spec.append("""
			create table tbl_Measurements(
				mes_msm_id				integer		primary key,
				mes_msm_date 			text,
				mes_dst_addr 			text,
				mes_af					int,
				mes_proto 				text,
				mes_msm_class 			text
			);
			""")
	db_spec.append("""
			create table tbl_results(
				res_num					integer 	primary key		autoincrement,
				res_msm_id				int,
				res_set_id 				int,
				res_err_id				int,
				res_type 				text,
				res_size 				int,
				res_timestamp 			int,
				res_src_addr 			text,
				res_prb_id 				text,
				res_fw					int,
				res_dst_name			text,
				res_from				text,
				res_ttl					int,
				res_paris_id			int,
				res_rcvd				int,	
				res_sent				int,
				res_avg 				text,
				res_ANCOUNT				int,
				res_ARCOUNT				int,
				res_ID					int,
				res_NSCOUNT				int,
				res_QCOUNT				int,
				res_abuf				text
			);
			""")
	db_spec.append("""
			create table tbl_result_set(
				set_num					integer		primary key		autoincrement,
				set_set_id				int,
				set_err_id				int,
				set_ttl					int,
				set_rtt 				text,
				set_srcaddr 			text,
				set_hop					int,
				set_size				int,
				set_from				text,
				set_icmpext_mpls_id		int,
				set_icmpext_class		int,
				set_icmpext_rfc4884		int,
				set_icmpext_version		text,
				set_icmpext_ittl		text,
				set_icmpext_rtt			text,
				set_icmpext_size		int,
				set_icmpext_ttl			int,
				set_icmpext_type		text,
				set_err_reason			text,
				set_err_value			text
			);
			""")
	db_spec.append("""
			create table tbl_traceroute_mpls(
				mpl_num 				integer 	primary key		autoincrement,
				mpl_icmptext_mpls_id 	int,
				mpl_exp					text,
				mpl_label				text,
				mpl_s					text,
				mpl_ttl					int
			);
			""")
	db_spec.append("""
			create table tbl_probes(
				id						int			primary key,
				asn						int,
				asn_v4					int,
				asn_v6					int,
				address_v4				text,
				address_v6				text,
				is_public				text,
				status					int,
				status_name				text,
				satus_since				int,
				country_code			text,
				location				text,
				latitude				text,
				longitude				text
			);
			""")
	for x in db_spec:
		cursor.execute(x)
	conn.commit()
	return conn


if __name__ == "__main__":
	conn = create_db()
