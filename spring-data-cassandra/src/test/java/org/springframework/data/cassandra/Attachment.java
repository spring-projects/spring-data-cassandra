/*
 * Copyright 2023-2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyClass;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.data.cassandra.core.mapping.UserDefinedType;

import java.util.Date;

@lombok.Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(Attachment.TABLE_NAME)
public class Attachment {

    public static final String TABLE_NAME = "attachment";

    public static final String PARENT_ID = "parent_id";
    public static final String DATA = "data";
    @Getter
    @Setter
    @PrimaryKey
    private Key id;

    @Getter
    @Setter
    @CassandraType(type = CassandraType.Name.BIGINT)
    @Column(PARENT_ID)
    private Long parentId;

    @Getter
    @Setter
    @Column(DATA)
    private AttachmentData data;

    @lombok.Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @PrimaryKeyClass
    public static class Key {

        public static final String MESSAGE_ID = "message_id";
        public static final String ATTACHMENT_ID = "attachment_id";

        /**
         * ID сообщения.
         *
         * {@code GEPS.ATTACHMENT.MESSAGE_ID}
         */
        @PrimaryKeyColumn(value = MESSAGE_ID, type = PrimaryKeyType.PARTITIONED, ordinal = 0)
        private long messageId;

        /**
         * ID вложения.
         *
         * {@code GEPS.ATTACHMENT.ATTACHMENT_ID}
         */
        @PrimaryKeyColumn(value = ATTACHMENT_ID, type = PrimaryKeyType.CLUSTERED, ordinal = 1)
        private long attachmentId;

    }

    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @EqualsAndHashCode
    @ToString
    @UserDefinedType(value = Attachment.AttachmentData.TYPE_NAME)
    public static class AttachmentData {

        public static final String TYPE_NAME = "attachment_data";

        public static final String DT_FILE_NAME = "file_name";
        public static final String DT_FILE_SIZE = "file_size";
        public static final String DT_MIME_TYPE = "mime_type";
        public static final String DT_SPF_BLANK_ID = "spf_blank_id";
        public static final String DT_STATUS = "status";
        public static final String DT_EXTERNAL_LINK = "external_link";
        public static final String DT_CREATE_DATE = "create_date";
        public static final String DT_UPDATE_DATE = "update_date";
        public static final String DT_BASE_DIR_ID = "base_dir_id";

        /**
         * Исходное имя файла вложения.
         *
         * {@code GEPS.ATTACHMENT_FILE_NAME}
         */
        @Getter
        @Setter
        @CassandraType(type = CassandraType.Name.VARCHAR)
        @Column(DT_FILE_NAME)
        private String fileName;

        /**
         * Размер файла вложения.
         * Может быть не заполнен т.к. размер не известен на этапе добавления сообщения в БД (при скачивании из ИС ПР).
         *
         * {@code GEPS.ATTACHMENT.FILE_SIZE}.
         */
        @Getter
        @Setter
        @CassandraType(type = CassandraType.Name.BIGINT)
        @Column(DT_FILE_SIZE)
        private Long fileSize;

        /**
         * MIME-type файла вложения.
         *
         * {@code GEPS.ATTACHMENT.MIME_TYPE}
         */
        @Getter
        @Setter
        @CassandraType(type = CassandraType.Name.VARCHAR)
        @Column(DT_MIME_TYPE)
        private String mimeType;

        /**
         * Код бланка сервера печатных форм для создания pdf представления вложения.
         *
         * {@code GEPS.ATTACHMENT.SPF_BLANK_ID}.
         */
        @Getter
        @Setter
        @CassandraType(type = CassandraType.Name.VARCHAR)
        @Column(DT_SPF_BLANK_ID)
        private String spfBlankId;

        /**
         * Статус загрузки вложения.
         * {@code GEPS.ATTACHMENT.STATUS}.
         */
        @Getter
        @Setter
        @CassandraType(type = CassandraType.Name.ASCII)
        @Column(DT_STATUS)
        private String status;

        /**
         * Ссылка на внешний файл вложения.
         *
         * {@code GEPS.ATTACHMENT.EXTERNAL_LINK}.
         */
        @Getter
        @Setter
        @CassandraType(type = CassandraType.Name.VARCHAR)
        @Column(DT_EXTERNAL_LINK)
        private String externalLink;

        /**
         * ID маппера для сохранения вложений.
         *
         * {@code GEPS.ATTACHMENT.BASE_DIR_ID}.
         */
        @Getter
        @Setter
        @CassandraType(type = CassandraType.Name.SMALLINT)
        @Column(DT_BASE_DIR_ID)
        private Short baseDirId;

        /**
         * Время создания (используется при переносе из Oracle)
         *
         * {@code GEPS.ATTACHMENT_SIGNATURE.CREATE_DATE}.
         */
        @Getter
        @Setter
        @CassandraType(type = CassandraType.Name.TIMESTAMP)
        @Column(DT_CREATE_DATE)
        private Date createDate;

        /**
         * Время обновления (используется при переносе из Oracle)
         *
         * {@code GEPS.ATTACHMENT_SIGNATURE.UPDATE_DATE}.
         */
        @Getter
        @Setter
        @CassandraType(type = CassandraType.Name.TIMESTAMP)
        @Column(DT_UPDATE_DATE)
        private Date updateDate;

    }

    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @EqualsAndHashCode
    @ToString
    @UserDefinedType(value = Attachment.AttachmentSignature.TYPE_NAME)
    public static class AttachmentSignature {

        public static final String TYPE_NAME = "attachment_signature";

        public static final String DT_STATUS = "eds_status";
        public static final String DT_START_DATE = "eds_start_date";
        public static final String DT_FINISH_DATE = "eds_finish_date";
        public static final String DT_CREATE_DATE = "create_date";
        public static final String DT_UPDATE_DATE = "update_date";
        public static final String DT_FILE_NAME = "file_name";
        public static final String DT_BASE_DIR_ID = "eds_base_dir";

        /**
         * Статус EDS-верификации вложения.
         * GET_DICTIONARY_ELEMENT_MNEMONIC(EDS_STATUS)
         * GEPS.ATTACHMENT_SIGNATURE.EDS_STATUS
         *
         * <table>
         *     <tr>
         *         <td>NEW</td><td>EDS-верификации не запускалась</td>
         *     </tr>
         *     <tr>
         *         <td>RUNNING</td><td>EDS-верификация подписи вложении запущена, продолжается</td>
         *     </tr>
         *     <tr>
         *         <td>OK</td><td>EDS-верификация подписи вложения успешно проведена в ГУЦ СМЭВ3</td>
         *     </tr>
         *     <tr>
         *         <td>ERROR</td><td>EDS-верификация завершилась с ошибкой</td>
         *     </tr>
         * </table>
         *
         * {@code GEPS.ATTACHMENT_SIGNATURE.EDS_STATUS}.
         */
        @Getter
        @Setter
        @CassandraType(type = CassandraType.Name.ASCII)
        @Column(DT_STATUS)
        private String edsStatus;

        /**
         * Время запуска EDS-проверки.
         *
         * {@code GEPS.ATTACHMENT_SIGNATURE.EDS_START_DATE}.
         */
        @Getter
        @Setter
        @CassandraType(type = CassandraType.Name.TIMESTAMP)
        @Column(DT_START_DATE)
        private Date edsStartDate;

        /**
         * Время завершения EDS-проверки.
         *
         * {@code GEPS.ATTACHMENT_SIGNATURE.EDS_FINISH_DATE}.
         */
        @Getter
        @Setter
        @CassandraType(type = CassandraType.Name.TIMESTAMP)
        @Column(DT_FINISH_DATE)
        private Date edsFinishDate;



        /**
         * {@code GEPS.ATTACHMENT.CREATE_DATE}.
         */
        @Getter
        @Setter
        @CassandraType(type = CassandraType.Name.TIMESTAMP)
        @Column(DT_CREATE_DATE)
        private Date createDate;

        /**
         * {@code GEPS.ATTACHMENT.UPDATE_DATE}.
         */
        @Getter
        @Setter
        @CassandraType(type = CassandraType.Name.TIMESTAMP)
        @Column(DT_UPDATE_DATE)
        private Date updateDate;

        /**
         * Имя файла eds-подписи вложения.
         *
         * {@code GEPS.ATTACHMENT_SIGNATURE.FILE_NAME}
         */
        @Getter
        @Setter
        @CassandraType(type = CassandraType.Name.VARCHAR)
        @Column(DT_FILE_NAME)
        private String edsFileName;

        /**
         * ID маппера для сохранения EDS-подписи вложений.
         *
         * {@code GEPS.ATTACHMENT_SIGNATURE.EDS_BASE_DIR_ID}.
         */
        @Getter
        @Setter
        @CassandraType(type = CassandraType.Name.SMALLINT)
        @Column(DT_BASE_DIR_ID)
        private Short edsBaseDirId;

    }

}
