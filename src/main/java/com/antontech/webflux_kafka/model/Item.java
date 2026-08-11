package com.antontech.webflux_kafka.model;

import com.google.gson.annotations.SerializedName;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * R2DBC entity mapped to the {@code ITEM} table.
 *
 * <h2>JPA → R2DBC migration notes</h2>
 * <ul>
 *   <li>{@code @Entity} is removed – R2DBC does not use JPA/Hibernate.</li>
 *   <li>{@code @Table} comes from {@code org.springframework.data.relational.core.mapping}, not JPA.</li>
 *   <li>{@code @Id} comes from {@code org.springframework.data.annotation}, not JPA.</li>
 *   <li>{@code @Column} comes from {@code org.springframework.data.relational.core.mapping}.</li>
 *   <li>Validation annotations ({@code @NotNull}) still work via {@code spring-boot-starter-validation}.</li>
 *   <li>The same MySQL table created by {@code sql-scripts/02_mysql_item_source_seed_200.sql} is used.</li>
 *   <li>Getters/setters are retained (no Lombok {@code @Data}) to keep Gson/Jackson serialisation predictable.</li>
 * </ul>
 *
 * <h2>Why R2DBC instead of JPA?</h2>
 * <p>
 * JPA/Hibernate uses JDBC which is <em>blocking</em>: the calling thread waits until the
 * database responds. R2DBC is a non-blocking, reactive database driver: the query is
 * submitted and the result is delivered as a {@link reactor.core.publisher.Mono} or
 * {@link reactor.core.publisher.Flux}, allowing Netty's event-loop thread to handle
 * other requests while the DB does its work.
 */
@Table("ITEM")
public class Item implements Serializable {

    @Id
    @Column("item_id")
    @SerializedName("itemId")
    private String itemId;

    @Column("item_level")
    @SerializedName("itemLevel")
    private Integer itemLevel;

    @Column("item_number_type")
    @SerializedName("itemNumberType")
    private String itemNumberType;

    @Column("prefix")
    @SerializedName("prefix")
    private Integer prefix;

    @Column("allocator_system")
    @SerializedName("allocatorSystem")
    private String allocatorSystem;

    @Column("business_unit_id")
    @SerializedName("businessUnitId")
    private Integer businessUnitId;

    @Column("catch_weight_ind")
    @SerializedName("catchWeightInd")
    private String catchWeightInd;

    @Column("class_id")
    @SerializedName("classId")
    private Integer classId;

    @Column("colour_dsc")
    @SerializedName("colourDsc")
    private String colourDsc;

    @Column("colour_group_id")
    @SerializedName("colourGroupId")
    private String colourGroupId;

    @Column("colour_id")
    @SerializedName("colourId")
    private String colourId;

    @Column("colour_range_id")
    @SerializedName("colourRangeId")
    private Integer colourRangeId;

    @Column("company_id")
    @SerializedName("companyId")
    private Integer companyId;

    @Column("count_on_us_id")
    @SerializedName("countOnUsId")
    private String countOnUsId;

    @Column("create_dte")
    @SerializedName("createDte")
    private LocalDateTime createDte;

    @Column("dept_id")
    @SerializedName("deptId")
    private Integer deptId;

    @Column("discipline")
    @SerializedName("discipline")
    private String discipline;

    @Column("domain_id")
    @SerializedName("domainId")
    private Integer domainId;

    @Column("flavour_dsc")
    @SerializedName("flavourDsc")
    private String flavourDsc;

    @Column("flavour_group_id")
    @SerializedName("flavourGroupId")
    private String flavourGroupId;

    @Column("flavour_id")
    @SerializedName("flavourId")
    private String flavourId;

    @Column("flavour_range_id")
    @SerializedName("flavourRangeId")
    private Integer flavourRangeId;

    @Column("forecast_ind")
    @SerializedName("forecastInd")
    private String forecastInd;

    @Column("free_range_id")
    @SerializedName("freeRangeId")
    private String freeRangeId;

    @Column("from_temp")
    @SerializedName("fromTemp")
    private Integer fromTemp;

    @Column("group_id")
    @SerializedName("groupId")
    private Integer groupId;

    @Column("high_max_temp")
    @SerializedName("highMaxTemp")
    private Integer highMaxTemp;

    @Column("high_min_temp")
    @SerializedName("highMinTemp")
    private Integer highMinTemp;

    @Column("item_grandparent")
    @SerializedName("itemGrandparent")
    private String itemGrandparent;

    @Column("item_parent")
    @SerializedName("itemParent")
    private String itemParent;

    @Column("kidz_id")
    @SerializedName("kidzId")
    private String kidzId;

    @Column("orderable_ind")
    @SerializedName("orderableInd")
    private String orderableInd;

    @Column("pack_ind")
    @SerializedName("packInd")
    private String packInd;

    @Column("pack_member")
    @SerializedName("packMember")
    private String packMember;

    @Column("pack_qty")
    @SerializedName("packQty")
    private BigDecimal packQty;

    @Column("phase_id")
    @SerializedName("phaseId")
    private Integer phaseId;

    @Column("price_mark_ind")
    @SerializedName("priceMarkInd")
    private String priceMarkInd;

    @Column("primary_ref_item_ind")
    @SerializedName("primaryRefItemInd")
    private String primaryRefItemInd;

    @Column("primary_size_dsc")
    @SerializedName("primarySizeDsc")
    private String primarySizeDsc;

    @Column("primary_size_group_id")
    @SerializedName("primarySizeGroupId")
    private String primarySizeGroupId;

    @Column("primary_size_id")
    @SerializedName("primarySizeId")
    private String primarySizeId;

    @Column("primary_size_range_id")
    @SerializedName("primarySizeRangeId")
    private Integer primarySizeRangeId;

    @Column("product_group_scaling")
    @SerializedName("productGroupScaling")
    private String productGroupScaling;

    @Column("product_id")
    @SerializedName("productId")
    private String productId;

    @Column("reference_item_ind")
    @SerializedName("referenceItemInd")
    private String referenceItemInd;

    @Column("scent_dsc")
    @SerializedName("scentDsc")
    private String scentDsc;

    @Column("scent_group_id")
    @SerializedName("scentGroupId")
    private String scentGroupId;

    @Column("scent_id")
    @SerializedName("scentId")
    private String scentId;

    @Column("scent_range_id")
    @SerializedName("scentRangeId")
    private Integer scentRangeId;

    @Column("season_id")
    @SerializedName("seasonId")
    private Integer seasonId;

    @Column("secondary_size_dsc")
    @SerializedName("secondarySizeDsc")
    private String secondarySizeDsc;

    @Column("secondary_size_group_id")
    @SerializedName("secondarySizeGroupId")
    private String secondarySizeGroupId;

    @Column("secondary_size_id")
    @SerializedName("secondarySizeId")
    private String secondarySizeId;

    @Column("secondary_size_range_id")
    @SerializedName("secondarySizeRangeId")
    private Integer secondarySizeRangeId;

    @Column("sellable_ind")
    @SerializedName("sellableInd")
    private String sellableInd;

    @Column("short_dsc")
    @SerializedName("shortDsc")
    private String shortDsc;

    @Column("simple_pack_ind")
    @SerializedName("simplePackInd")
    private String simplePackInd;

    @Column("size_profile_ind")
    @SerializedName("sizeProfileInd")
    private String sizeProfileInd;

    @Column("standard_uom")
    @SerializedName("standardUom")
    private String standardUom;

    @Column("status")
    @SerializedName("status")
    private String status;

    @Column("sub_group_id")
    @SerializedName("subGroupId")
    private Integer subGroupId;

    @Column("subclass_id")
    @SerializedName("subclassId")
    private Integer subclassId;

    @Column("supplier_no")
    @SerializedName("supplierNo")
    private Integer supplierNo;

    @Column("to_temp")
    @SerializedName("toTemp")
    private Integer toTemp;

    @Column("tran_ind")
    @SerializedName("tranInd")
    private String tranInd;

    @Column("tran_level")
    @SerializedName("tranLevel")
    private Integer tranLevel;

    @Column("ww_colour")
    @SerializedName("wwColour")
    private String wwColour;

    @Column("ww_size")
    @SerializedName("wwSize")
    private String wwSize;

    @Column("ww_static_mass")
    @SerializedName("wwStaticMass")
    private BigDecimal wwStaticMass;

    @Column("ww_style")
    @SerializedName("wwStyle")
    private String wwStyle;

    @Column("ww_style_colour")
    @SerializedName("wwStyleColour")
    private String wwStyleColour;

    @Column("variable_weight_ind")
    @SerializedName("variableWeightInd")
    private Character variableWeightInd;

    @Column("loose_prod_ind")
    @SerializedName("looseProdInd")
    private Character looseProdInd;

    @Column("item_scale_ind")
    @SerializedName("itemScaleInd")
    private Character itemScaleInd;

    @Column("legacy_sku_no")
    @SerializedName("legacySkuNo")
    private String legacySkuNo;

    @Column("legacy_random_mass_ind")
    @SerializedName("legacyRandomMassInd")
    private Character legacyRandomMassInd;

    @Column("legacy_vat_ind")
    @SerializedName("legacyVatInd")
    private Character legacyVatInd;

    @Column("action_ind")
    @SerializedName("actionInd")
    private Character actionInd;

    @Column("extract_seq_no")
    @SerializedName("extractSeqNo")
    private Long extractSeqNo;

    @Column("vat_cde")
    @SerializedName("vatCde")
    private String vatCde;

    @Column("vat_rate")
    @SerializedName("vatRate")
    private BigDecimal vatRate;

    @Column("source_system")
    @SerializedName("sourceSystem")
    private String sourceSystem;

    @Column("vpn_no")
    @SerializedName("vpnNo")
    private String vpnNo;

    @Column("ext_ref_no")
    @SerializedName("extRefNo")
    private String extRefNo;

    @Column("item_long_desc")
    @SerializedName("itemLongDesc")
    private String itemLongDesc;

    @Column("segregation_ind")
    @SerializedName("segregationInd")
    private String segregationInd;

    @Column("prod_class")
    @SerializedName("prodClass")
    private String prodClass;

    @Column("last_update_dte")
    @SerializedName("lastUpdateDte")
    private LocalDateTime lastUpdateDte;

    // --- Getters and Setters ---

    public String getItemId() { return itemId; }
    public void setItemId(String itemId) { this.itemId = itemId; }
    public Integer getItemLevel() { return itemLevel; }
    public void setItemLevel(Integer itemLevel) { this.itemLevel = itemLevel; }
    public String getItemNumberType() { return itemNumberType; }
    public void setItemNumberType(String itemNumberType) { this.itemNumberType = itemNumberType; }
    public Integer getPrefix() { return prefix; }
    public void setPrefix(Integer prefix) { this.prefix = prefix; }
    public String getAllocatorSystem() { return allocatorSystem; }
    public void setAllocatorSystem(String allocatorSystem) { this.allocatorSystem = allocatorSystem; }
    public Integer getBusinessUnitId() { return businessUnitId; }
    public void setBusinessUnitId(Integer businessUnitId) { this.businessUnitId = businessUnitId; }
    public String getCatchWeightInd() { return catchWeightInd; }
    public void setCatchWeightInd(String catchWeightInd) { this.catchWeightInd = catchWeightInd; }
    public Integer getClassId() { return classId; }
    public void setClassId(Integer classId) { this.classId = classId; }
    public String getColourDsc() { return colourDsc; }
    public void setColourDsc(String colourDsc) { this.colourDsc = colourDsc; }
    public String getColourGroupId() { return colourGroupId; }
    public void setColourGroupId(String colourGroupId) { this.colourGroupId = colourGroupId; }
    public String getColourId() { return colourId; }
    public void setColourId(String colourId) { this.colourId = colourId; }
    public Integer getColourRangeId() { return colourRangeId; }
    public void setColourRangeId(Integer colourRangeId) { this.colourRangeId = colourRangeId; }
    public Integer getCompanyId() { return companyId; }
    public void setCompanyId(Integer companyId) { this.companyId = companyId; }
    public String getCountOnUsId() { return countOnUsId; }
    public void setCountOnUsId(String countOnUsId) { this.countOnUsId = countOnUsId; }
    public LocalDateTime getCreateDte() { return createDte; }
    public void setCreateDte(LocalDateTime createDte) { this.createDte = createDte; }
    public Integer getDeptId() { return deptId; }
    public void setDeptId(Integer deptId) { this.deptId = deptId; }
    public String getDiscipline() { return discipline; }
    public void setDiscipline(String discipline) { this.discipline = discipline; }
    public Integer getDomainId() { return domainId; }
    public void setDomainId(Integer domainId) { this.domainId = domainId; }
    public String getFlavourDsc() { return flavourDsc; }
    public void setFlavourDsc(String flavourDsc) { this.flavourDsc = flavourDsc; }
    public String getFlavourGroupId() { return flavourGroupId; }
    public void setFlavourGroupId(String flavourGroupId) { this.flavourGroupId = flavourGroupId; }
    public String getFlavourId() { return flavourId; }
    public void setFlavourId(String flavourId) { this.flavourId = flavourId; }
    public Integer getFlavourRangeId() { return flavourRangeId; }
    public void setFlavourRangeId(Integer flavourRangeId) { this.flavourRangeId = flavourRangeId; }
    public String getForecastInd() { return forecastInd; }
    public void setForecastInd(String forecastInd) { this.forecastInd = forecastInd; }
    public String getFreeRangeId() { return freeRangeId; }
    public void setFreeRangeId(String freeRangeId) { this.freeRangeId = freeRangeId; }
    public Integer getFromTemp() { return fromTemp; }
    public void setFromTemp(Integer fromTemp) { this.fromTemp = fromTemp; }
    public Integer getGroupId() { return groupId; }
    public void setGroupId(Integer groupId) { this.groupId = groupId; }
    public Integer getHighMaxTemp() { return highMaxTemp; }
    public void setHighMaxTemp(Integer highMaxTemp) { this.highMaxTemp = highMaxTemp; }
    public Integer getHighMinTemp() { return highMinTemp; }
    public void setHighMinTemp(Integer highMinTemp) { this.highMinTemp = highMinTemp; }
    public String getItemGrandparent() { return itemGrandparent; }
    public void setItemGrandparent(String itemGrandparent) { this.itemGrandparent = itemGrandparent; }
    public String getItemParent() { return itemParent; }
    public void setItemParent(String itemParent) { this.itemParent = itemParent; }
    public String getKidzId() { return kidzId; }
    public void setKidzId(String kidzId) { this.kidzId = kidzId; }
    public String getOrderableInd() { return orderableInd; }
    public void setOrderableInd(String orderableInd) { this.orderableInd = orderableInd; }
    public String getPackInd() { return packInd; }
    public void setPackInd(String packInd) { this.packInd = packInd; }
    public String getPackMember() { return packMember; }
    public void setPackMember(String packMember) { this.packMember = packMember; }
    public BigDecimal getPackQty() { return packQty; }
    public void setPackQty(BigDecimal packQty) { this.packQty = packQty; }
    public Integer getPhaseId() { return phaseId; }
    public void setPhaseId(Integer phaseId) { this.phaseId = phaseId; }
    public String getPriceMarkInd() { return priceMarkInd; }
    public void setPriceMarkInd(String priceMarkInd) { this.priceMarkInd = priceMarkInd; }
    public String getPrimaryRefItemInd() { return primaryRefItemInd; }
    public void setPrimaryRefItemInd(String primaryRefItemInd) { this.primaryRefItemInd = primaryRefItemInd; }
    public String getPrimarySizeDsc() { return primarySizeDsc; }
    public void setPrimarySizeDsc(String primarySizeDsc) { this.primarySizeDsc = primarySizeDsc; }
    public String getPrimarySizeGroupId() { return primarySizeGroupId; }
    public void setPrimarySizeGroupId(String primarySizeGroupId) { this.primarySizeGroupId = primarySizeGroupId; }
    public String getPrimarySizeId() { return primarySizeId; }
    public void setPrimarySizeId(String primarySizeId) { this.primarySizeId = primarySizeId; }
    public Integer getPrimarySizeRangeId() { return primarySizeRangeId; }
    public void setPrimarySizeRangeId(Integer primarySizeRangeId) { this.primarySizeRangeId = primarySizeRangeId; }
    public String getProductGroupScaling() { return productGroupScaling; }
    public void setProductGroupScaling(String productGroupScaling) { this.productGroupScaling = productGroupScaling; }
    public String getProductId() { return productId; }
    public void setProductId(String productId) { this.productId = productId; }
    public String getReferenceItemInd() { return referenceItemInd; }
    public void setReferenceItemInd(String referenceItemInd) { this.referenceItemInd = referenceItemInd; }
    public String getScentDsc() { return scentDsc; }
    public void setScentDsc(String scentDsc) { this.scentDsc = scentDsc; }
    public String getScentGroupId() { return scentGroupId; }
    public void setScentGroupId(String scentGroupId) { this.scentGroupId = scentGroupId; }
    public String getScentId() { return scentId; }
    public void setScentId(String scentId) { this.scentId = scentId; }
    public Integer getScentRangeId() { return scentRangeId; }
    public void setScentRangeId(Integer scentRangeId) { this.scentRangeId = scentRangeId; }
    public Integer getSeasonId() { return seasonId; }
    public void setSeasonId(Integer seasonId) { this.seasonId = seasonId; }
    public String getSecondarySizeDsc() { return secondarySizeDsc; }
    public void setSecondarySizeDsc(String secondarySizeDsc) { this.secondarySizeDsc = secondarySizeDsc; }
    public String getSecondarySizeGroupId() { return secondarySizeGroupId; }
    public void setSecondarySizeGroupId(String secondarySizeGroupId) { this.secondarySizeGroupId = secondarySizeGroupId; }
    public String getSecondarySizeId() { return secondarySizeId; }
    public void setSecondarySizeId(String secondarySizeId) { this.secondarySizeId = secondarySizeId; }
    public Integer getSecondarySizeRangeId() { return secondarySizeRangeId; }
    public void setSecondarySizeRangeId(Integer secondarySizeRangeId) { this.secondarySizeRangeId = secondarySizeRangeId; }
    public String getSellableInd() { return sellableInd; }
    public void setSellableInd(String sellableInd) { this.sellableInd = sellableInd; }
    public String getShortDsc() { return shortDsc; }
    public void setShortDsc(String shortDsc) { this.shortDsc = shortDsc; }
    public String getSimplePackInd() { return simplePackInd; }
    public void setSimplePackInd(String simplePackInd) { this.simplePackInd = simplePackInd; }
    public String getSizeProfileInd() { return sizeProfileInd; }
    public void setSizeProfileInd(String sizeProfileInd) { this.sizeProfileInd = sizeProfileInd; }
    public String getStandardUom() { return standardUom; }
    public void setStandardUom(String standardUom) { this.standardUom = standardUom; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public Integer getSubGroupId() { return subGroupId; }
    public void setSubGroupId(Integer subGroupId) { this.subGroupId = subGroupId; }
    public Integer getSubclassId() { return subclassId; }
    public void setSubclassId(Integer subclassId) { this.subclassId = subclassId; }
    public Integer getSupplierNo() { return supplierNo; }
    public void setSupplierNo(Integer supplierNo) { this.supplierNo = supplierNo; }
    public Integer getToTemp() { return toTemp; }
    public void setToTemp(Integer toTemp) { this.toTemp = toTemp; }
    public String getTranInd() { return tranInd; }
    public void setTranInd(String tranInd) { this.tranInd = tranInd; }
    public Integer getTranLevel() { return tranLevel; }
    public void setTranLevel(Integer tranLevel) { this.tranLevel = tranLevel; }
    public String getWwColour() { return wwColour; }
    public void setWwColour(String wwColour) { this.wwColour = wwColour; }
    public String getWwSize() { return wwSize; }
    public void setWwSize(String wwSize) { this.wwSize = wwSize; }
    public BigDecimal getWwStaticMass() { return wwStaticMass; }
    public void setWwStaticMass(BigDecimal wwStaticMass) { this.wwStaticMass = wwStaticMass; }
    public String getWwStyle() { return wwStyle; }
    public void setWwStyle(String wwStyle) { this.wwStyle = wwStyle; }
    public String getWwStyleColour() { return wwStyleColour; }
    public void setWwStyleColour(String wwStyleColour) { this.wwStyleColour = wwStyleColour; }
    public Character getVariableWeightInd() { return variableWeightInd; }
    public void setVariableWeightInd(Character variableWeightInd) { this.variableWeightInd = variableWeightInd; }
    public Character getLooseProdInd() { return looseProdInd; }
    public void setLooseProdInd(Character looseProdInd) { this.looseProdInd = looseProdInd; }
    public Character getItemScaleInd() { return itemScaleInd; }
    public void setItemScaleInd(Character itemScaleInd) { this.itemScaleInd = itemScaleInd; }
    public String getLegacySkuNo() { return legacySkuNo; }
    public void setLegacySkuNo(String legacySkuNo) { this.legacySkuNo = legacySkuNo; }
    public Character getLegacyRandomMassInd() { return legacyRandomMassInd; }
    public void setLegacyRandomMassInd(Character legacyRandomMassInd) { this.legacyRandomMassInd = legacyRandomMassInd; }
    public Character getLegacyVatInd() { return legacyVatInd; }
    public void setLegacyVatInd(Character legacyVatInd) { this.legacyVatInd = legacyVatInd; }
    public Character getActionInd() { return actionInd; }
    public void setActionInd(Character actionInd) { this.actionInd = actionInd; }
    public Long getExtractSeqNo() { return extractSeqNo; }
    public void setExtractSeqNo(Long extractSeqNo) { this.extractSeqNo = extractSeqNo; }
    public String getVatCde() { return vatCde; }
    public void setVatCde(String vatCde) { this.vatCde = vatCde; }
    public BigDecimal getVatRate() { return vatRate; }
    public void setVatRate(BigDecimal vatRate) { this.vatRate = vatRate; }
    public String getSourceSystem() { return sourceSystem; }
    public void setSourceSystem(String sourceSystem) { this.sourceSystem = sourceSystem; }
    public String getVpnNo() { return vpnNo; }
    public void setVpnNo(String vpnNo) { this.vpnNo = vpnNo; }
    public String getExtRefNo() { return extRefNo; }
    public void setExtRefNo(String extRefNo) { this.extRefNo = extRefNo; }
    public String getItemLongDesc() { return itemLongDesc; }
    public void setItemLongDesc(String itemLongDesc) { this.itemLongDesc = itemLongDesc; }
    public String getSegregationInd() { return segregationInd; }
    public void setSegregationInd(String segregationInd) { this.segregationInd = segregationInd; }
    public String getProdClass() { return prodClass; }
    public void setProdClass(String prodClass) { this.prodClass = prodClass; }
    public LocalDateTime getLastUpdateDte() { return lastUpdateDte; }
    public void setLastUpdateDte(LocalDateTime lastUpdateDte) { this.lastUpdateDte = lastUpdateDte; }
}

