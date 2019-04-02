package azure

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/Azure/azure-sdk-for-go/services/preview/hdinsight/mgmt/2018-06-01-preview/hdinsight"
	"github.com/hashicorp/go-getter/helper/url"
	"github.com/hashicorp/terraform/helper/schema"
	"github.com/hashicorp/terraform/helper/validation"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/helpers/validate"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/utils"
)

func SchemaHDInsightTier() *schema.Schema {
	return &schema.Schema{
		Type:     schema.TypeString,
		Required: true,
		ForceNew: true,
		ValidateFunc: validation.StringInSlice([]string{
			string(hdinsight.Standard),
			string(hdinsight.Premium),
		}, false),
		// TODO: file a bug about this
		DiffSuppressFunc: SuppressLocationDiff,
	}
}

func SchemaHDInsightClusterVersion() *schema.Schema {
	return &schema.Schema{
		Type:     schema.TypeString,
		Required: true,
		ForceNew: true,
		DiffSuppressFunc: func(k, old, new string, d *schema.ResourceData) bool {
			// TODO: tests
			// `3.6` gets converted to `3.6.1000.67`; so let's just compare major/minor if possible
			o := strings.Split(old, ".")
			n := strings.Split(new, ".")

			if len(o) >= 2 && len(n) >= 2 {
				oldMajor := o[0]
				oldMinor := o[1]
				newMajor := n[0]
				newMinor := n[1]

				return oldMajor == newMajor && oldMinor == newMinor
			}

			return false
		},
	}
}

func SchemaHDInsightsGateway() *schema.Schema {
	return &schema.Schema{
		Type:     schema.TypeList,
		Required: true,
		MaxItems: 1,
		Elem: &schema.Resource{
			Schema: map[string]*schema.Schema{
				"enabled": {
					Type:     schema.TypeBool,
					Required: true,
					ForceNew: true,
				},
				// NOTE: these are Required since if these aren't present you get a `500 bad request`
				"username": {
					Type:     schema.TypeString,
					Required: true,
					ForceNew: true,
				},
				"password": {
					Type:      schema.TypeString,
					Required:  true,
					ForceNew:  true,
					Sensitive: true,
				},
			},
		},
	}
}

func ExpandHDInsightsConfigurations(input []interface{}) map[string]interface{} {
	vs := input[0].(map[string]interface{})

	// NOTE: Admin username must be different from SSH Username
	enabled := vs["enabled"].(bool)
	username := vs["username"].(string)
	password := vs["password"].(string)

	return map[string]interface{}{
		"gateway": map[string]interface{}{
			"restAuthCredential.isEnabled": enabled,
			"restAuthCredential.username":  username,
			"restAuthCredential.password":  password,
		},
	}
}

func FlattenHDInsightsConfigurations(input map[string]*string) []interface{} {
	enabled := false
	if v, exists := input["restAuthCredential.isEnabled"]; exists && v != nil {
		e, err := strconv.ParseBool(*v)
		if err == nil {
			enabled = e
		}
	}

	username := ""
	if v, exists := input["restAuthCredential.username"]; exists && v != nil {
		username = *v
	}

	password := ""
	if v, exists := input["restAuthCredential.password"]; exists && v != nil {
		password = *v
	}

	return []interface{}{
		map[string]interface{}{
			"enabled":  enabled,
			"username": username,
			"password": password,
		},
	}
}

func SchemaHDInsightsStorageAccounts() *schema.Schema {
	return &schema.Schema{
		Type:     schema.TypeList,
		Required: true,
		Elem: &schema.Resource{
			Schema: map[string]*schema.Schema{
				"storage_account_key": {
					Type:      schema.TypeString,
					Required:  true,
					ForceNew:  true,
					Sensitive: true,
				},
				"storage_container_id": {
					Type:     schema.TypeString,
					Required: true,
					ForceNew: true,
				},
				"is_default": {
					Type:     schema.TypeBool,
					Required: true,
					ForceNew: true,
				},
			},
		},
	}
}

func ExpandHDInsightsStorageAccounts(input []interface{}) (*[]hdinsight.StorageAccount, error) {
	results := make([]hdinsight.StorageAccount, 0)

	for _, vs := range input {
		v := vs.(map[string]interface{})

		storageAccountKey := v["storage_account_key"].(string)
		storageContainerId := v["storage_container_id"].(string)
		isDefault := v["is_default"].(bool)

		// https://foo.blob.core.windows.net/example
		uri, err := url.Parse(storageContainerId)
		if err != nil {
			return nil, fmt.Errorf("Error parsing %q: %s", storageContainerId, err)
		}

		result := hdinsight.StorageAccount{
			Name:      utils.String(uri.Host),
			Container: utils.String(strings.TrimPrefix(uri.Path, "/")),
			Key:       utils.String(storageAccountKey),
			IsDefault: utils.Bool(isDefault),
		}
		results = append(results, result)
	}

	return &results, nil
}

func SchemaHDInsightNodeDefinition(name string, canSpecifyCount bool, minCount, maxCount int, validVMSizes []string) *schema.Schema {
	result := map[string]*schema.Schema{
		// TODO: Network
		"vm_size": {
			Type:     schema.TypeString,
			Required: true,
			ForceNew: true,
			// TODO: re-enable me
			//ValidateFunc: validation.StringInSlice(validVMSizes, false),
		},
		"username": {
			Type:     schema.TypeString,
			Required: true,
			ForceNew: true,
		},
		"password": {
			Type:      schema.TypeString,
			Optional:  true,
			ForceNew:  true,
			Sensitive: true,
			// TODO: conflictswith ssh keys
			// The password must be at least 10 characters in length and must contain at least one digit, one uppercase and one lower case letter, one non-alphanumeric character (except characters ' " ` \).
		},
		"ssh_keys": {
			// TODO: determine if this is ForceNew or not
			Type:     schema.TypeSet,
			Optional: true,
			ForceNew: true,
			Elem: &schema.Schema{
				Type:         schema.TypeString,
				ValidateFunc: validate.NoEmptyStrings,
			},
			Set: schema.HashString,
			ConflictsWith: []string{
				fmt.Sprintf("%s.0.password", name),
			},
		},
	}

	if canSpecifyCount {
		result["min_instance_count"] = &schema.Schema{
			// TODO: is this ForceNew?
			Type:         schema.TypeInt,
			Optional:     true,
			ValidateFunc: validation.IntBetween(minCount, maxCount),
		}
		result["target_instance_count"] = &schema.Schema{
			Type:         schema.TypeInt,
			Required:     true,
			ValidateFunc: validation.IntBetween(minCount, maxCount),
		}
	}

	return &schema.Schema{
		Type:     schema.TypeList,
		Required: true,
		MaxItems: 1,
		Elem: &schema.Resource{
			Schema: result,
		},
	}
}

func ExpandHDInsightNodeDefinition(name string, input []interface{}, canSpecifyCount bool, fixedMinCount *int32, fixedTargetCount *int32) (*hdinsight.Role, error) {
	v := input[0].(map[string]interface{})
	vmSize := v["vm_size"].(string)
	username := v["username"].(string)
	password := v["password"].(string)

	role := hdinsight.Role{
		Name: utils.String(name),
		HardwareProfile: &hdinsight.HardwareProfile{
			VMSize: utils.String(vmSize),
		},
		OsProfile: &hdinsight.OsProfile{
			LinuxOperatingSystemProfile: &hdinsight.LinuxOperatingSystemProfile{
				Username: utils.String(username),
			},
		},
	}

	if password != "" {
		role.OsProfile.LinuxOperatingSystemProfile.Password = utils.String(password)
	} else {
		sshKeysRaw := v["ssh_keys"].(*schema.Set).List()
		sshKeys := make([]hdinsight.SSHPublicKey, 0)
		for _, v := range sshKeysRaw {
			sshKeys = append(sshKeys, hdinsight.SSHPublicKey{
				CertificateData: utils.String(v.(string)),
			})
		}

		if len(sshKeys) == 0 {
			return nil, fmt.Errorf("Either a `password` or `ssh_key` must be specified!")
		}

		role.OsProfile.LinuxOperatingSystemProfile.SSHProfile = &hdinsight.SSHProfile{
			PublicKeys: &sshKeys,
		}
	}

	if canSpecifyCount {
		minInstanceCount := v["min_instance_count"].(int)
		if minInstanceCount > 0 {
			role.MinInstanceCount = utils.Int32(int32(minInstanceCount))
		}

		targetInstanceCount := v["target_instance_count"].(int)
		role.TargetInstanceCount = utils.Int32(int32(targetInstanceCount))
	} else {
		role.MinInstanceCount = fixedMinCount
		role.TargetInstanceCount = fixedTargetCount
	}

	return &role, nil
}

func FlattenHDInsightNodeDefinition(input *hdinsight.Role, canSetCount bool, existing []interface{}, skuOverrides map[string]string) []interface{} {
	if input == nil {
		return []interface{}{}
	}

	output := map[string]interface{}{
		"vm_size":  "",
		"username": "",
		"password": "",
		"ssh_keys": schema.NewSet(schema.HashString, []interface{}{}),
	}

	if profile := input.OsProfile; profile != nil {
		if osProfile := profile.LinuxOperatingSystemProfile; osProfile != nil {
			if username := osProfile.Username; username != nil {
				output["username"] = *username
			}

			// TODO: SSH Keys if they're returned
		}
	}

	if len(existing) > 0 {
		existingV := existing[0].(map[string]interface{})
		output["password"] = existingV["password"].(string)
	}

	if profile := input.HardwareProfile; profile != nil {
		if size := profile.VMSize; size != nil {
			skuName := *size
			if v, ok := skuOverrides[strings.ToLower(skuName)]; ok {
				skuName = v
			}
			output["vm_size"] = skuName
		}
	}

	if canSetCount {
		output["min_instance_count"] = 0
		output["target_instance_count"] = 0

		if input.MinInstanceCount != nil {
			output["min_instance_count"] = int(*input.MinInstanceCount)
		}

		if input.TargetInstanceCount != nil {
			output["target_instance_count"] = int(*input.TargetInstanceCount)
		}
	}

	// TODO: are SSH keys returned?

	return []interface{}{output}
}

func FindHDInsightRole(input *[]hdinsight.Role, name string) *hdinsight.Role {
	if input == nil {
		return nil
	}

	for _, v := range *input {
		if v.Name == nil {
			continue
		}

		actualName := *v.Name
		if strings.EqualFold(name, actualName) {
			return &v
		}
	}

	return nil
}
