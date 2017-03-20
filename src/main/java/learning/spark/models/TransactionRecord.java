package learning.spark.models;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.UUID;

@Table(name = "trx_rec")
@Entity
public class TransactionRecord implements Serializable {

    @Id
    private Long id;

    @Column(name="reference_id")
    private Long referenceId;

    @Column(name="requested")
    private Long requested;

    @Column(name="processed")
    private Long processed;

    @Column(name = "type_id")
    private Integer type;

    @Column(name = "tran_curr_id")
    private Integer tranCurrency;

    @Column(name = "tran_amt", precision = 20, scale = 2)
    private BigDecimal tranAmount;

    @Column(name = "merch_trx_id", length = 256)
    private String merchantTransactionId;

    @Column(name = "merch_trx_ref")
    private String merchantTransactionReference;

    @Column(name = "unique_id")
    private UUID uuid;

    @Column(name = "channel_id", nullable = false, columnDefinition = "binary(16)")
    private UUID channelId;

    @Column(name = "merchant_id", columnDefinition = "binary(16)")
    private UUID merchantId;

    @Column(name = "short_id", nullable = false)
    private long shortId;

    @Column(name = "binding_snapshot_id", nullable = true)
    private Long bindingSnapshotId;

    @Column(name = "invoiceId", nullable = true, length=255)
    private String invoiceId;

    @Column(name = "shopperId", nullable = true, length=255)
    private String shopperId;

    public TransactionRecord() {

    }

    public TransactionRecord(Long id, Long referenceId, Long requested, Long processed, Integer type, Integer tranCurrency, BigDecimal tranAmount, String merchantTransactionId, String merchantTransactionReference, UUID uuid, UUID channelId, UUID merchantId, long shortId, Long bindingSnapshotId, String invoiceId, String shopperId) {
        this.id = id;
        this.referenceId = referenceId;
        this.requested = requested;
        this.processed = processed;
        this.type = type;
        this.tranCurrency = tranCurrency;
        this.tranAmount = tranAmount;
        this.merchantTransactionId = merchantTransactionId;
        this.merchantTransactionReference = merchantTransactionReference;
        this.uuid = uuid;
        this.channelId = channelId;
        this.merchantId = merchantId;
        this.shortId = shortId;
        this.bindingSnapshotId = bindingSnapshotId;
        this.invoiceId = invoiceId;
        this.shopperId = shopperId;
    }

    public TransactionRecord(UUID channelId, UUID merchantId, Long referenceTransaction,
                             long requestedInMillis, Integer type, Integer tranCurrency, BigDecimal tranAmount,
                             String merchantTransactionId, String invoiceId, String shopperId)
    {
        this.channelId = channelId;
        this.merchantId = merchantId;
        this.referenceId = referenceTransaction;
        this.requested = requestedInMillis;
        this.type = type;
        this.tranCurrency = tranCurrency;
        this.tranAmount = tranAmount;
        this.merchantTransactionId = merchantTransactionId;
        this.uuid = UUID.randomUUID();
        this.shopperId = shopperId;
        this.invoiceId = invoiceId;
    }

    public Long getId() {
        return id;
    }

    public Long getReferenceId() {
        return referenceId;
    }

    public Long getRequested() {
        return requested;
    }

    public Long getProcessed() {
        return processed;
    }

    public Integer getType() {
        return type;
    }

    public Integer getTranCurrency() {
        return tranCurrency;
    }

    public BigDecimal getTranAmount() {
        return tranAmount;
    }

    public String getMerchantTransactionId() {
        return merchantTransactionId;
    }

    public String getMerchantTransactionReference() {
        return merchantTransactionReference;
    }

    public UUID getUuid() {
        return uuid;
    }

    public UUID getChannelId() {
        return channelId;
    }

    public UUID getMerchantId() {
        return merchantId;
    }

    public long getShortId() {
        return shortId;
    }

    public Long getBindingSnapshotId() {
        return bindingSnapshotId;
    }

    public String getInvoiceId() {
        return invoiceId;
    }

    public String getShopperId() {
        return shopperId;
    }
}
